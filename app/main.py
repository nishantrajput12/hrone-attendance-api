"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:   uvicorn app.main:app --port 8000
Env:   MONGO_URI, MONGO_DB   (a local .env is read for convenience; real environment variables always win)

Layout of this file (everything lives here, as the assignment requires):
    1. configuration          - environment, Mongo client, constants
    2. time + rule helpers    - R1-R5 (IST dates, late, overtime, work hours, half day) and R8 rounding
    3. (de)serialisation      - BSON datetime <-> epoch milliseconds, documents -> API shapes
    4. request models         - pydantic models with strict validation
    5. indexes + startup      - idempotent index creation
    6. query/pipeline builders- one builder per endpoint, shared by the endpoint AND /admin/explain
    7. endpoints
"""
import calendar
import json
import logging
import math
import os
import threading
import time as time_module
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, NamedTuple, Optional

from bson import Decimal128, json_util
from bson.errors import BSONError
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import AfterValidator, BaseModel, Field, StringConstraints, model_validator
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError
from fastapi.exception_handlers import http_exception_handler as default_http_exception_handler
from starlette.convertors import Convertor, register_url_convertor
from starlette.exceptions import HTTPException as StarletteHTTPException

# =========================================================================== #
# 1. Configuration
# =========================================================================== #
load_dotenv()  # override=False: a real environment variable always beats the .env file

log = logging.getLogger("attendance")


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set. Provide it as an environment variable or in a local .env file.")
    return value


# tz_aware=True makes PyMongo return timezone-aware (UTC) datetimes instead of naive ones, so we never have to guess.
client = MongoClient(
    _required_env("MONGO_URI"),
    tz_aware=True,
    tzinfo=timezone.utc,
    serverSelectionTimeoutMS=5000,
    appname="attendance-api",
)
db = client[_required_env("MONGO_DB")]
employees = db["employees"]
logs = db["attendance_logs"]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENT_STATUSES = ["PRESENT", "WFH", "ON_DUTY"]  # R6
NO_WORK_STATUSES = ("ABSENT", "LEAVE")
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

GRACE_SECONDS = 10 * 60  # R2
MIN_OVERTIME_MINUTES = 30  # R3
HALF_DAY_BELOW_HOURS = 4.5  # R5
MAX_PUNCH_GAP = timedelta(hours=24)
MAX_TREND_DAYS = 92
MIN_EPOCH_MS = 100_000_000_000
MAX_EPOCH_MS = 4_102_444_800_000

# Index names (also used as query hints so the planner can never fall back to a collection scan)
IDX_EMP_CODE = "emp_code_1"
IDX_EMP_DEPT = "department_1_emp_code_1"
IDX_EMP_JOINED = "joined_on_1"
IDX_LOG_EMP_DATE = "emp_code_1_date_1"
IDX_LOG_DATE_EMP = "date_-1_emp_code_1"
IDX_LOG_STATUS_DATE = "status_1_date_-1_emp_code_1"
MAX_SKIP = 2**62  # keeps (page - 1) * page_size inside what MongoDB accepts (signed 64-bit) for absurdly large pages


# =========================================================================== #
# 2. Time + rule helpers (R1 - R5, R8)
# =========================================================================== #
def ms_to_dt(ms: int) -> datetime:
    """Epoch milliseconds -> aware UTC datetime. Integer arithmetic only, so no float rounding."""
    return EPOCH + timedelta(milliseconds=ms)


def dt_to_ms(dt: datetime) -> int:
    """Datetime -> epoch milliseconds. A naive datetime is treated as UTC (that is what MongoDB stores)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    delta = dt - EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def now_ms() -> int:
    return time_module.time_ns() // 1_000_000


def truncate_to_second_ms(ms: int) -> int:
    """R1: instants are truncated to whole seconds before any calculation, and the truncated value is stored."""
    return ms - ms % 1000


def hm_to_minutes(hhmm: str) -> int:
    hours, minutes = hhmm.split(":")
    return int(hours) * 60 + int(minutes)


def is_overnight(shift_start: str, shift_end: str) -> bool:
    """R1: shift_end <= shift_start means the shift ends on the next calendar day."""
    return hm_to_minutes(shift_end) <= hm_to_minutes(shift_start)


def shift_datetime(day: date, hhmm: str) -> datetime:
    """The IST instant at `hhmm` on calendar day `day`."""
    minutes = hm_to_minutes(hhmm)
    return datetime.combine(day, time(minutes // 60, minutes % 60), tzinfo=IST)


def attendance_date_for(punch_in: datetime, emp: dict) -> date:
    """R1: the record's date is the IST calendar date of the punch-in.
    Overnight shifts: a punch-in earlier than shift_end belongs to the shift that started the day before."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if is_overnight(emp["shift_start"], emp["shift_end"]):
        seconds_into_day = local.hour * 3600 + local.minute * 60 + local.second
        if seconds_into_day < hm_to_minutes(emp["shift_end"]) * 60:
            day -= timedelta(days=1)
    return day


def compute_late_minutes(punch_in: datetime, att_date: date, shift_start: str) -> int:
    """R2: late only if STRICTLY more than 10:00 after shift_start; then whole minutes (floor) since shift_start."""
    seconds = int((punch_in - shift_datetime(att_date, shift_start)).total_seconds())
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def compute_overtime_minutes(punch_out: datetime, att_date: date, shift_start: str, shift_end: str) -> int:
    """R3: whole minutes (floor) from shift_end to punch_out, counted only when that is >= 30."""
    end = shift_datetime(att_date, shift_end)
    if is_overnight(shift_start, shift_end):
        end += timedelta(days=1)
    minutes = int((punch_out - end).total_seconds()) // 60  # floor division also floors negatives
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600 rounded to 2 decimals HALF-UP, done with integers so there is no float error.
    hundredths = floor(seconds / 36 + 1/2) = (seconds + 18) // 36."""
    seconds = int((punch_out - punch_in).total_seconds())
    return ((seconds + 18) // 36) / 100


def derive_fields(emp: dict, att_date: date, punch_in: Optional[datetime], punch_out: Optional[datetime]) -> dict:
    """The four derived fields (R2-R5) for a record with these punch times."""
    if punch_in is None:  # ABSENT / LEAVE
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = compute_late_minutes(punch_in, att_date, emp["shift_start"])
    if punch_out is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    hours = compute_work_hours(punch_in, punch_out)
    return {
        "work_hours": hours,
        "late_minutes": late,
        "overtime_minutes": compute_overtime_minutes(punch_out, att_date, emp["shift_start"], emp["shift_end"]),
        "half_day": hours < HALF_DAY_BELOW_HOURS,  # R5 uses the ROUNDED work_hours
    }


def to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal128):
        return value.to_decimal()
    return Decimal(str(value))


def round_half_up(value: Decimal, places: int) -> float:
    """R8: half-up rounding (not banker's rounding, not float rounding)."""
    return float(value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def month_bounds(month: str) -> tuple:
    year, mon = int(month[:4]), int(month[5:7])
    return date(year, mon, 1), date(year, mon, calendar.monthrange(year, mon)[1])


def weekend_dates(first: date, last: date) -> list:
    """The Saturdays and Sundays between two dates, as 'YYYY-MM-DD' strings (R7: working days are Mon-Fri)."""
    days = (last - first).days + 1
    return [d.isoformat() for d in (first + timedelta(days=i) for i in range(days)) if d.weekday() >= 5]


def count_working_days(first: date, last: date, joined_on: Optional[str]) -> int:
    """R7: Mon-Fri days in the month, counted from joined_on for a mid-month joiner."""
    start = first
    if joined_on:
        try:
            start = max(first, date.fromisoformat(joined_on))
        except ValueError:
            pass
    if start > last:
        return 0
    return sum(1 for i in range((last - start).days + 1) if (start + timedelta(days=i)).weekday() < 5)


# =========================================================================== #
# 3. (De)serialisation
# =========================================================================== #
def _ms_or_none(value: Any) -> Optional[int]:
    return dt_to_ms(value) if isinstance(value, datetime) else None


def _json_value(value: Any) -> Any:
    return dt_to_ms(value) if isinstance(value, datetime) else value


def serialize_history_entry(entry: dict) -> dict:
    changes = {}
    for field, change in (entry.get("changes") or {}).items():
        changes[field] = {"from": _json_value(change.get("from")), "to": _json_value(change.get("to"))}
    return {"at": _ms_or_none(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"), "changes": changes}


def serialize_record(doc: dict) -> dict:
    """An attendance_logs document -> the AttendanceRecord in the contract. Missing legacy fields get their defaults."""
    work_hours = doc.get("work_hours")
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": _ms_or_none(doc.get("punch_in")),
        "punch_out": _ms_or_none(doc.get("punch_out")),
        "work_hours": float(work_hours) if work_hours is not None else None,
        "late_minutes": int(doc.get("late_minutes") or 0),
        "overtime_minutes": int(doc.get("overtime_minutes") or 0),
        "half_day": bool(doc.get("half_day") or False),
        "history": [serialize_history_entry(h) for h in (doc.get("history") or [])],
    }


def serialize_employee(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": _ms_or_none(doc.get("created_at")),
    }


def unprocessable(message: str, where: str = "body") -> RequestValidationError:
    """A 422 raised from business rules, in the same shape FastAPI uses for schema errors."""
    return RequestValidationError([{"loc": [where], "msg": message, "type": "value_error"}])


# =========================================================================== #
# 4. Request models and validated parameter types
# =========================================================================== #
def _check_date(value: str) -> str:
    if len(value) != 10 or not all(c in "0123456789-" for c in value) or value[4] != "-" or value[7] != "-":
        raise ValueError("must be a date in YYYY-MM-DD format")
    try:
        date.fromisoformat(value)
    except ValueError:
        raise ValueError("not a real calendar date")
    return value


def _check_month(value: str) -> str:
    if len(value) != 7 or value[4] != "-" or not (value[:4] + value[5:]).isascii() or not (value[:4] + value[5:]).isdigit():
        raise ValueError("must be a month in YYYY-MM format")
    if not 1 <= int(value[5:7]) <= 12 or int(value[:4]) < 1:
        raise ValueError("not a real month")
    return value


def _check_epoch_ms(value: int) -> int:
    if not MIN_EPOCH_MS <= value <= MAX_EPOCH_MS:  # below the minimum it looks like seconds, not milliseconds
        raise ValueError("must be an integer number of epoch MILLISECONDS")
    return value


def _check_text(value: str) -> str:
    """A JSON string may contain a lone surrogate ("\\ud800"). Python accepts it but BSON cannot store it, which would be a 500."""
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("contains characters that cannot be stored (invalid Unicode)")
    return value


DateStr = Annotated[str, AfterValidator(_check_date)]
MonthStr = Annotated[str, AfterValidator(_check_month)]
HHMM = Annotated[str, StringConstraints(strict=True, pattern=r"^([01][0-9]|2[0-3]):[0-5][0-9]$")]
# strict=True: floats such as 1783312500000.0, numeric strings and booleans are rejected (422), not coerced
EpochMs = Annotated[int, Field(strict=True), AfterValidator(_check_epoch_ms)]
SafeText = Annotated[str, StringConstraints(strict=True), AfterValidator(_check_text)]
PageParam = Annotated[int, Query(ge=1)]
PageSizeParam = Annotated[int, Query(ge=1, le=100)]


class EmployeeCreate(BaseModel):
    emp_code: Annotated[str, StringConstraints(strict=True, pattern=r"^EMP[0-9]{4,6}$")]
    name: Annotated[str, StringConstraints(strict=True, min_length=1, max_length=100), AfterValidator(_check_text)]
    email: Annotated[
        str,
        StringConstraints(strict=True, max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
        AfterValidator(_check_text),
    ]
    department: Annotated[str, StringConstraints(strict=True, min_length=1, max_length=50), AfterValidator(_check_text)]
    shift_start: HHMM = "09:30"
    shift_end: HHMM = "18:30"
    joined_on: Annotated[str, StringConstraints(strict=True), AfterValidator(_check_date)]

    @model_validator(mode="after")
    def _shift_times_differ(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInRequest(BaseModel):
    emp_code: SafeText
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutRequest(BaseModel):
    emp_code: SafeText
    punched_at: Optional[EpochMs] = None


class RegularizeRequest(BaseModel):
    # Derived fields (work_hours, late_minutes, ...) are not declared, so a client that sends them is simply ignored.
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: Annotated[str, StringConstraints(strict=True, min_length=5, max_length=200), AfterValidator(_check_text)]
    regularized_by: Annotated[str, StringConstraints(strict=True, min_length=1, max_length=50), AfterValidator(_check_text)]


# =========================================================================== #
# 5. Indexes and application start-up
# =========================================================================== #
_INDEXES = [
    # employees
    (employees, [("emp_code", 1)], {"name": IDX_EMP_CODE, "unique": True}),  # identity + 409 on duplicates
    (employees, [("department", 1), ("emp_code", 1)], {"name": IDX_EMP_DEPT}),  # filter by department, sorted by emp_code
    (employees, [("joined_on", 1)], {"name": IDX_EMP_JOINED}),  # headcount "joined on or before" without a department
    # attendance_logs
    (logs, [("emp_code", 1), ("date", 1)], {"name": IDX_LOG_EMP_DATE, "unique": True}),  # one record per day + per-employee queries
    (logs, [("date", -1), ("emp_code", 1)], {"name": IDX_LOG_DATE_EMP}),  # list sort order, month/day ranges, leaderboard
    (logs, [("status", 1), ("date", -1), ("emp_code", 1)], {"name": IDX_LOG_STATUS_DATE}),  # list filtered by status
]
_indexes_ready = False
_index_lock = threading.Lock()


def ensure_indexes() -> bool:
    """Create every index. Idempotent: creating an index that already exists is a no-op."""
    global _indexes_ready
    with _index_lock:
        if _indexes_ready:
            return True
        ok = True
        for collection, keys, options in _INDEXES:
            try:
                collection.create_index(keys, **options)
            except PyMongoError:
                ok = False
                log.exception("could not create index %s on %s", options["name"], collection.name)
        _indexes_ready = ok
        return ok


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except Exception:  # never stop the app from starting; /health reports the database state
        log.exception("index creation failed at startup")
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request, exc: RequestValidationError):
    # Only plain, JSON-safe fields: pydantic's error "ctx" can hold exception objects that cannot be serialised.
    detail = [
        {"loc": list(e.get("loc", [])), "msg": str(e.get("msg", "")), "type": str(e.get("type", ""))}
        for e in exc.errors()
    ]
    return JSONResponse(status_code=422, content={"detail": detail})


@app.exception_handler(StarletteHTTPException)
async def http_error_handler(request: Request, exc: StarletteHTTPException):
    # A body that is not even decodable (e.g. invalid UTF-8 bytes) makes FastAPI answer 400 "There was an error parsing
    # the body". The contract calls an invalid body a validation failure, which is 422. Everything else is unchanged.
    if exc.status_code == 400 and exc.detail == "There was an error parsing the body":
        detail = [{"loc": ["body"], "msg": "the request body could not be decoded", "type": "value_error"}]
        return JSONResponse(status_code=422, content={"detail": detail})
    return await default_http_exception_handler(request, exc)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_: Request, exc: PyMongoError):
    log.error("database error: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


@app.exception_handler(UnicodeError)
@app.exception_handler(OverflowError)
@app.exception_handler(BSONError)
async def unstorable_value_handler(_: Request, exc: Exception):
    # Safety net: a value the client sent cannot be encoded for MongoDB (invalid Unicode, number too large). That is the
    # client's problem, so it is a 422 and never a 500.
    log.warning("rejected an unstorable value: %s", exc)
    return JSONResponse(status_code=422, content={"detail": [{"loc": ["request"], "msg": "a value cannot be stored", "type": "value_error"}]})


# =========================================================================== #
# 6. Query / pipeline builders (shared by the real endpoints and /admin/explain)
# =========================================================================== #
class Plan(NamedTuple):
    collection: str
    kind: str  # "find" or "aggregate"
    spec: dict  # find: {filter, sort, skip, limit}   aggregate: {pipeline}
    hint: str


def _present_halves(weekend_list: Optional[list] = None, prefix: str = "") -> dict:
    """Expression: 2 for a full present day, 1 for a half day, 0 otherwise (R5, R6, R7).
    Whole numbers keep every sum exact; divide by 2 at the end. When weekend_list is given, weekend records add 0.
    `prefix` is the path of the log document, e.g. "logs." when the logs were embedded by a $lookup."""
    value = {"$cond": [{"$eq": [{"$ifNull": [f"${prefix}half_day", False]}, True]}, 1, 2]}
    condition: dict = {"$in": [f"${prefix}status", PRESENT_STATUSES]}
    if weekend_list is not None:
        condition = {"$and": [condition, {"$not": [{"$in": [f"${prefix}date", weekend_list]}]}]}
    return {"$cond": [condition, value, 0]}


def _is_late() -> dict:
    return {"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}


def build_attendance_list(emp_code, date_from, date_to, status, page, page_size) -> Plan:
    flt: dict = {}
    if emp_code is not None:
        flt["emp_code"] = emp_code
    if date_from is not None or date_to is not None:
        flt["date"] = {}
        if date_from is not None:
            flt["date"]["$gte"] = date_from
        if date_to is not None:
            flt["date"]["$lte"] = date_to
    if status is not None:
        flt["status"] = status
    spec = {
        "filter": flt,
        "sort": {"date": -1, "emp_code": 1},
        "skip": min((page - 1) * page_size, MAX_SKIP),
        "limit": page_size,
    }
    # Pick the index that serves the filter AND the sort (date DESC, emp_code ASC) without an in-memory sort:
    #   emp_code given -> the per-employee index, walked backwards;  status given -> the status index;  else the date index.
    if emp_code is not None:
        hint = IDX_LOG_EMP_DATE
    elif status is not None:
        hint = IDX_LOG_STATUS_DATE
    else:
        hint = IDX_LOG_DATE_EMP
    return Plan("attendance_logs", "find", spec, hint)


def build_employee_monthly(emp_code: str, month: str) -> Plan:
    first, last = month_bounds(month)
    weekends = weekend_dates(first, last)
    pipeline = [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {
            "$group": {
                "_id": None,
                "halves": {"$sum": _present_halves(weekends)},
                "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "late_count": {"$sum": {"$cond": [_is_late(), 1, 0]}},
                "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
                "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            }
        },
    ]
    return Plan("attendance_logs", "aggregate", {"pipeline": pipeline}, IDX_LOG_EMP_DATE)


def build_department_summary(month: str, department: Optional[str]) -> Plan:
    first, last = month_bounds(month)
    weekends = weekend_dates(first, last)
    has_hours = {"$and": [{"$in": ["$status", PRESENT_STATUSES]}, {"$isNumber": "$work_hours"}]}
    match: dict = {"joined_on": {"$lte": last.isoformat()}}  # R9: headcount = joined on or before the end of the month
    if department is not None:
        match["department"] = department
    per_employee = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {
            "$group": {
                "_id": None,
                "halves": {"$sum": _present_halves(weekends)},
                "work_n": {"$sum": {"$cond": [has_hours, 1, 0]}},
                "work_sum": {"$sum": {"$cond": [has_hours, {"$toDecimal": "$work_hours"}, 0]}},  # Decimal128: exact sums
                "late_count": {"$sum": {"$cond": [_is_late(), 1, 0]}},
                "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
                "leave_count": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}},
            }
        },
    ]
    pipeline = [
        # Start from employees: that is what makes people with ZERO logs still count towards the headcount.
        {"$match": match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": per_employee,
                "as": "agg",
            }
        },
        {"$set": {"agg": {"$ifNull": [{"$first": "$agg"}, {}]}}},  # no logs -> empty doc -> every sum below adds 0
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "halves": {"$sum": "$agg.halves"},
                "work_n": {"$sum": "$agg.work_n"},
                "work_sum": {"$sum": "$agg.work_sum"},
                "late_count": {"$sum": "$agg.late_count"},
                "total_late_minutes": {"$sum": "$agg.total_late_minutes"},
                "leave_count": {"$sum": "$agg.leave_count"},
                "on_duty_count": {"$sum": "$agg.on_duty_count"},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    hint = IDX_EMP_DEPT if department is not None else IDX_EMP_JOINED
    return Plan("employees", "aggregate", {"pipeline": pipeline}, hint)


def build_late_leaderboard(month: str, limit: int, department: Optional[str]) -> Plan:
    first, last = month_bounds(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs whose employee does not exist are dropped here
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department only
    pipeline += [
        # $rank = standard competition ranking: ties share a rank, the next rank is skipped (1, 2, 2, 4)
        {"$setWindowFields": {"sortBy": {"total": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies AFTER ranking, so ties at the cutoff are all kept
        {"$sort": {"total": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$emp.name",
                "department": "$emp.department",
                "total_late_minutes": "$total",
                "late_count": 1,
            }
        },
    ]
    return Plan("attendance_logs", "aggregate", {"pipeline": pipeline}, IDX_LOG_DATE_EMP)


def build_department_trend(department: str, d_from: date, d_to: date) -> Plan:
    n_days = (d_to - d_from).days + 1
    start = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    pipeline = [
        {"$match": {"department": department}},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": d_from.isoformat(), "$lte": d_to.isoformat()}}},
                    {"$project": {"_id": 0, "date": 1, "status": 1, "late_minutes": 1, "half_day": 1}},
                ],
                "as": "logs",
            }
        },
        # One pass over the department's employees feeds two small summaries: joiners per date and activity per date.
        {
            "$facet": {
                "joiners": [{"$group": {"_id": "$joined_on", "n": {"$sum": 1}}}],
                "daily": [
                    {"$unwind": "$logs"},
                    {
                        "$group": {
                            "_id": "$logs.date",
                            "halves": {"$sum": _present_halves(prefix="logs.")},  # any weekday or weekend
                            "late_count": {"$sum": {"$cond": [{"$gt": [{"$ifNull": ["$logs.late_minutes", 0]}, 0]}, 1, 0]}},
                        }
                    },
                ],
            }
        },
        # Gap filling inside the database: one document per calendar day from `from` to `to`, whether or not it has data.
        {
            "$project": {
                "joiners": 1,
                "daily": 1,
                "days": {
                    "$map": {
                        "input": {"$range": [0, n_days]},
                        "as": "i",
                        "in": {"$dateAdd": {"startDate": start, "unit": "day", "amount": "$$i"}},
                    }
                },
            }
        },
        {"$unwind": "$days"},
        {
            "$set": {
                "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$days"}},
                "dow": {"$dayOfWeek": "$days"},  # 1 = Sunday ... 7 = Saturday
            }
        },
        {
            "$set": {
                "cur": {"$first": {"$filter": {"input": "$daily", "cond": {"$eq": ["$$this._id", "$date"]}}}},
                "headcount": {  # R9: employees with joined_on <= this day
                    "$sum": {
                        "$map": {
                            "input": {"$filter": {"input": "$joiners", "cond": {"$lte": ["$$this._id", "$date"]}}},
                            "in": "$$this.n",
                        }
                    }
                },
                "is_working_day": {"$in": ["$dow", [2, 3, 4, 5, 6]]},
            }
        },
        {
            "$set": {
                "halves": {"$ifNull": ["$cur.halves", 0]},
                "late_count": {"$ifNull": ["$cur.late_count", 0]},
            }
        },
        # attendance_rate = present_count / headcount = halves / (2 x headcount), rounded HALF-UP to 4 places. MongoDB's own
        # $round is banker's rounding, so it is done with exact integer arithmetic instead, and the rate is held as a
        # whole number of ten-thousandths:   units = floor((10000 x halves + headcount) / (2 x headcount)).
        {
            "$set": {
                "rate_num": {"$add": [{"$multiply": ["$halves", 10000]}, "$headcount"]},
                "rate_den": {"$multiply": ["$headcount", 2]},
            }
        },
        {
            "$set": {
                "rate_units": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},  # null on weekends / empty departments
                        _floor_div("$rate_num", "$rate_den"),
                        None,
                    ]
                }
            }
        },
        # moving_avg_7d = mean of the published (rounded) non-null rates over the current row and up to 6 earlier rows of
        # the requested range. Sum and count come from the window; weekends (null) add to neither.
        {
            "$setWindowFields": {
                "sortBy": {"date": 1},
                "output": {
                    "win_sum": {"$sum": "$rate_units", "window": {"documents": [-6, 0]}},
                    "win_n": {"$sum": {"$cond": [{"$isNumber": "$rate_units"}, 1, 0]}, "window": {"documents": [-6, 0]}},
                },
            }
        },
        {
            "$set": {  # round the mean half-up as well: floor(sum / n + 1/2) = floor((2 x sum + n) / (2 x n))
                "moving_units": {
                    "$cond": [
                        {"$gt": ["$win_n", 0]},
                        _floor_div({"$add": [{"$multiply": ["$win_sum", 2]}, "$win_n"]}, {"$multiply": ["$win_n", 2]}),
                        None,
                    ]
                }
            }
        },
        {"$sort": {"date": 1}},
        {
            "$project": {
                "_id": 0,
                "date": 1,
                "is_working_day": 1,
                "headcount": 1,
                "halves": 1,
                "late_count": 1,
                "rate_units": 1,
                "moving_units": 1,
            }
        },
    ]
    return Plan("employees", "aggregate", {"pipeline": pipeline}, IDX_EMP_DEPT)


def _floor_div(numerator: Any, denominator: Any) -> dict:
    """Exact integer floor(numerator / denominator) in an aggregation expression (both are whole, non-negative numbers).
    (n - n mod d) is an exact multiple of d, so the division that follows has no rounding error."""
    return {"$toLong": {"$divide": [{"$subtract": [numerator, {"$mod": [numerator, denominator]}]}, denominator]}}


def run_aggregate(plan: Plan) -> list:
    return list(db[plan.collection].aggregate(plan.spec["pipeline"], hint=plan.hint))


def explain_command(plan: Plan) -> dict:
    """MongoDB `explain` (executionStats) of exactly what the endpoint runs."""
    if plan.kind == "find":
        command = {"find": plan.collection, **plan.spec, "hint": plan.hint}
    else:
        command = {"aggregate": plan.collection, "pipeline": plan.spec["pipeline"], "cursor": {}, "hint": plan.hint}
    return db.command({"explain": command, "verbosity": "executionStats"})


def _json_safe(value: Any) -> Any:
    """Make an explain document JSON-serialisable (dates, ObjectIds, Decimal128, NaN/Infinity)."""
    value = json.loads(json_util.dumps(value, json_options=json_util.RELAXED_JSON_OPTIONS))

    def clean(node):
        if isinstance(node, float) and not math.isfinite(node):
            return None
        if isinstance(node, dict):
            return {k: clean(v) for k, v in node.items()}
        if isinstance(node, list):
            return [clean(v) for v in node]
        return node

    return clean(value)


# =========================================================================== #
# 7. Endpoints
# =========================================================================== #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "MongoDB is not reachable")
    if not _indexes_ready:
        ensure_indexes()  # start-up may have happened before MongoDB was reachable
    return {"status": "ok"}


# ---- employees ------------------------------------------------------------ #
@app.post("/employees", status_code=201)
def create_employee(body: EmployeeCreate):
    doc = {
        "emp_code": body.emp_code,
        "name": body.name,
        "email": body.email,
        "department": body.department,
        "shift_start": body.shift_start,
        "shift_end": body.shift_end,
        "joined_on": body.joined_on,
        "created_at": ms_to_dt(now_ms()),
    }
    try:
        employees.insert_one(doc)  # the unique index decides: no check-then-insert race
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return serialize_employee(doc)


@app.get("/employees")
def list_employees(
    department: Annotated[Optional[str], Query()] = None,
    page: PageParam = 1,
    page_size: PageSizeParam = 20,
):
    flt = {} if department is None else {"department": department}
    total = employees.count_documents(flt) if flt else employees.estimated_document_count()
    skip = (page - 1) * page_size
    items = []
    if skip < total:  # also avoids sending an absurd skip value to MongoDB
        hint = IDX_EMP_DEPT if flt else IDX_EMP_CODE
        cursor = employees.find(flt, {"_id": 0}, hint=hint).sort("emp_code", 1).skip(skip).limit(page_size)
        items = [serialize_employee(d) for d in cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


# ---- attendance ------------------------------------------------------------ #
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInRequest):
    emp = employees.find_one({"emp_code": body.emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(404, "employee not found")
    punched_in = ms_to_dt(truncate_to_second_ms(body.punched_at if body.punched_at is not None else now_ms()))
    att_date = attendance_date_for(punched_in, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": att_date.isoformat(),
        "status": body.status,
        "punch_in": punched_in,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": compute_late_minutes(punched_in, att_date, emp["shift_start"]),
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }
    try:
        logs.insert_one(doc)  # unique (emp_code, date) index: exactly one of N simultaneous requests wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return serialize_record(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutRequest):
    emp = employees.find_one({"emp_code": body.emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(404, "employee not found")
    out_dt = ms_to_dt(truncate_to_second_ms(body.punched_at if body.punched_at is not None else now_ms()))
    # The most recent record whose punch-in is at or before the punch-out. Working from punch_in (not from "today")
    # is what makes an overnight shift close correctly after midnight.
    record = logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": out_dt}},
        sort=[("punch_in", -1)],
        hint=IDX_LOG_EMP_DATE,
    )
    if record is None:
        raise HTTPException(404, "no punch-in found")
    if record.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    p_in = record["punch_in"]
    if out_dt <= p_in:
        raise unprocessable("punched_at must be after the punch-in", "punched_at")
    if out_dt - p_in > MAX_PUNCH_GAP:
        raise unprocessable("punched_at is more than 24 hours after the punch-in", "punched_at")
    derived = derive_fields(emp, date.fromisoformat(record["date"]), p_in, out_dt)
    derived.pop("late_minutes")  # per the contract, late_minutes is set at punch-in (and by corrections), never here
    # Compare-and-set: only the writer that still sees an open record wins, so two simultaneous punch-outs give one 200.
    updated = logs.find_one_and_update(
        {"_id": record["_id"], "punch_out": None, "punch_in": p_in},
        {"$set": {"punch_out": out_dt, **derived}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return serialize_record(updated)


@app.get("/attendance")
def list_attendance(
    emp_code: Annotated[Optional[str], Query()] = None,
    date_from: Annotated[Optional[DateStr], Query()] = None,
    date_to: Annotated[Optional[DateStr], Query()] = None,
    status: Annotated[Optional[Status], Query()] = None,
    page: PageParam = 1,
    page_size: PageSizeParam = 20,
):
    if date_from is not None and date_to is not None and date_from > date_to:
        raise unprocessable("date_from must not be after date_to", "date_from")
    plan = build_attendance_list(emp_code, date_from, date_to, status, page, page_size)
    flt = plan.spec["filter"]
    total = logs.count_documents(flt, hint=plan.hint) if flt else logs.estimated_document_count()
    items = []
    if plan.spec["skip"] < total:
        cursor = (
            logs.find(flt, {"_id": 0}, hint=plan.hint)
            .sort([("date", -1), ("emp_code", 1)])
            .skip(plan.spec["skip"])
            .limit(page_size)
        )
        items = [serialize_record(d) for d in cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


def plan_regularization(emp: dict, record: dict, body: RegularizeRequest) -> tuple:
    """Validate a correction and work out the new stored values. Returns (new_values, changes)."""
    att_date = date.fromisoformat(record["date"])
    old_status = record["status"]
    old_in, old_out = record.get("punch_in"), record.get("punch_out")
    new_status = body.status or old_status
    times_supplied = body.punch_in is not None or body.punch_out is not None

    if new_status in NO_WORK_STATUSES:
        if times_supplied:
            raise unprocessable("punch_in / punch_out cannot be supplied with status ABSENT or LEAVE")
        new_in = new_out = None  # ABSENT / LEAVE clear both punch times
    else:
        new_in = ms_to_dt(truncate_to_second_ms(body.punch_in)) if body.punch_in is not None else old_in
        new_out = ms_to_dt(truncate_to_second_ms(body.punch_out)) if body.punch_out is not None else old_out
        if new_in is None:
            raise unprocessable("a presence status requires a punch_in")
        if body.punch_in is not None and attendance_date_for(new_in, emp) != att_date:
            raise unprocessable("punch_in must stay on the record's attendance date", "punch_in")
        if times_supplied and new_out is not None:
            if new_out <= new_in:
                raise unprocessable("punch_out must be after punch_in", "punch_out")
            if new_out - new_in > MAX_PUNCH_GAP:
                raise unprocessable("punch_out must be within 24 hours of punch_in", "punch_out")

    new_values = {"status": new_status, "punch_in": new_in, "punch_out": new_out}
    new_values.update(derive_fields(emp, att_date, new_in, new_out))  # recompute R2-R5 from the FINAL times
    old_values = {
        "status": old_status,
        "punch_in": old_in,
        "punch_out": old_out,
        "work_hours": record.get("work_hours"),
        "late_minutes": record.get("late_minutes") or 0,
        "overtime_minutes": record.get("overtime_minutes") or 0,
        "half_day": bool(record.get("half_day") or False),
    }

    def same(a, b):
        if isinstance(a, datetime) or isinstance(b, datetime):
            return isinstance(a, datetime) and isinstance(b, datetime) and dt_to_ms(a) == dt_to_ms(b)
        return a == b

    changes = {k: {"from": old_values[k], "to": new_values[k]} for k in new_values if not same(old_values[k], new_values[k])}
    if not changes:
        raise unprocessable("the request does not change the record")
    return new_values, changes


@app.patch("/attendance/{emp_code}/{date}")
def regularize_attendance(
    emp_code: str,
    # The URL variable is called "date" (the contract); the Python name differs only because `date` is the datetime class.
    # The alias MUST match the {date} in the route above, otherwise FastAPI answers 422 "path.date field required".
    record_date: Annotated[DateStr, Path(alias="date")],
    body: RegularizeRequest,
):
    emp = employees.find_one({"emp_code": emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(404, "employee not found")
    record = logs.find_one({"emp_code": emp_code, "date": record_date})
    if record is None:
        raise HTTPException(404, "no attendance record for that date")

    new_values, changes = plan_regularization(emp, record, body)
    entry = {"at": ms_to_dt(now_ms()), "by": body.regularized_by, "reason": body.reason, "changes": changes}

    # Optimistic concurrency: write only if the record is still exactly what we read (same status, punch times and
    # number of history entries). Two simultaneous corrections cannot both pass this filter, so no history entry is
    # ever lost; the loser gets 409 and can retry.
    history = record.get("history")  # legacy documents may have no history at all (or an explicit null)
    guard = {
        "_id": record["_id"],
        "status": record["status"],
        "punch_in": record.get("punch_in"),
        "punch_out": record.get("punch_out"),
        "history": {"$size": len(history)} if history is not None else None,  # None matches "missing" and "null"
    }
    if history is None:
        update = {"$set": {**new_values, "history": [entry]}}  # first ever entry: create the array
    else:
        update = {"$set": new_values, "$push": {"history": entry}}  # $push appends: history is never overwritten
    updated = logs.find_one_and_update(guard, update, return_document=ReturnDocument.AFTER)
    if updated is None:
        raise HTTPException(409, "the record was modified by another request; please retry")
    return serialize_record(updated)


# ---- analytics ------------------------------------------------------------- #
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: Annotated[MonthStr, Query()]):
    emp = employees.find_one({"emp_code": emp_code}, {"_id": 0, "joined_on": 1})
    if emp is None:
        raise HTTPException(404, "employee not found")
    first, last = month_bounds(month)
    rows = run_aggregate(build_employee_monthly(emp_code, month))
    row = rows[0] if rows else {}
    halves = int(row.get("halves", 0))
    working_days = count_working_days(first, last, emp.get("joined_on"))
    attendance_pct = None
    if working_days > 0:
        attendance_pct = round_half_up(Decimal(halves) * 50 / Decimal(working_days), 2)  # (halves / 2) / days * 100
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": halves / 2,
        "leave_days": int(row.get("leave_days", 0)),
        "late_count": int(row.get("late_count", 0)),
        "total_late_minutes": int(row.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(row.get("total_overtime_minutes", 0)),
        "attendance_pct": attendance_pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: Annotated[MonthStr, Query()], department: Annotated[Optional[str], Query()] = None):
    items = []
    for row in run_aggregate(build_department_summary(month, department)):
        work_n = int(row.get("work_n", 0))
        avg_hours = None
        if work_n > 0:
            avg_hours = round_half_up(to_decimal(row["work_sum"]) / Decimal(work_n), 2)  # mean over RECORDS
        items.append(
            {
                "department": row["_id"],
                "headcount": int(row["headcount"]),
                "present_days": int(row.get("halves", 0)) / 2,
                "avg_work_hours": avg_hours,
                "late_count": int(row.get("late_count", 0)),
                "total_late_minutes": int(row.get("total_late_minutes", 0)),
                "leave_count": int(row.get("leave_count", 0)),
                "on_duty_count": int(row.get("on_duty_count", 0)),
            }
        )
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: Annotated[MonthStr, Query()],
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    department: Annotated[Optional[str], Query()] = None,
):
    rows = run_aggregate(build_late_leaderboard(month, limit, department))
    return {"month": month, "items": rows}


class AnyTextConvertor(Convertor):
    """Matches ANY text in a URL segment, including "/" ("Ops/Support") and newlines. A plain {department} stops at "/";
    Starlette's built-in {department:path} uses '.*', which does not match a newline, so a name containing one would 404."""

    regex = "(?s:.*)"

    def convert(self, value: str) -> str:
        return value

    def to_string(self, value: str) -> str:
        return str(value)


register_url_convertor("anytext", AnyTextConvertor())


@app.get("/analytics/departments/{department:anytext}/trend")
def department_trend(
    department: str,
    from_date: Annotated[DateStr, Query(alias="from")],
    to_date: Annotated[DateStr, Query(alias="to")],
):
    d_from, d_to = date.fromisoformat(from_date), date.fromisoformat(to_date)
    _check_trend_range(d_from, d_to)
    if employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(404, "unknown department")
    items = []
    for row in run_aggregate(build_department_trend(department, d_from, d_to)):
        rate_units, moving_units = row.get("rate_units"), row.get("moving_units")  # whole ten-thousandths, already half-up
        items.append(
            {
                "date": row["date"],
                "is_working_day": bool(row["is_working_day"]),
                "headcount": int(row["headcount"]),
                "present_count": int(row.get("halves", 0)) / 2,
                "late_count": int(row.get("late_count", 0)),
                "attendance_rate": int(rate_units) / 10000 if rate_units is not None else None,
                "moving_avg_7d": int(moving_units) / 10000 if moving_units is not None else None,
            }
        )
    return {"department": department, "items": items}


def _check_trend_range(d_from: date, d_to: date) -> None:
    if d_to < d_from:
        raise unprocessable("'to' must not be before 'from'", "to")
    if (d_to - d_from).days + 1 > MAX_TREND_DAYS:
        raise unprocessable(f"the range must not exceed {MAX_TREND_DAYS} days", "to")


# ---- admin ----------------------------------------------------------------- #
ExplainTarget = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"]


def _require(**params) -> None:
    for name, value in params.items():
        if value is None:
            raise RequestValidationError(
                [{"loc": ["query", name], "msg": "field required for this endpoint", "type": "missing"}]
            )


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: ExplainTarget,
    emp_code: Annotated[Optional[str], Query()] = None,
    month: Annotated[Optional[MonthStr], Query()] = None,
    department: Annotated[Optional[str], Query()] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    date_from: Annotated[Optional[DateStr], Query()] = None,
    date_to: Annotated[Optional[DateStr], Query()] = None,
    status: Annotated[Optional[Status], Query()] = None,
    from_date: Annotated[Optional[DateStr], Query(alias="from")] = None,
    to_date: Annotated[Optional[DateStr], Query(alias="to")] = None,
    page: PageParam = 1,
    page_size: PageSizeParam = 20,
):
    if endpoint == "attendance_list":
        if date_from is not None and date_to is not None and date_from > date_to:
            raise unprocessable("date_from must not be after date_to", "date_from")
        plan = build_attendance_list(emp_code, date_from, date_to, status, page, page_size)
    elif endpoint == "employee_monthly":
        _require(emp_code=emp_code, month=month)
        plan = build_employee_monthly(emp_code, month)
    elif endpoint == "department_summary":
        _require(month=month)
        plan = build_department_summary(month, department)
    elif endpoint == "late_leaderboard":
        _require(month=month)
        plan = build_late_leaderboard(month, limit, department)
    else:  # department_trend
        _require(department=department, **{"from": from_date, "to": to_date})
        d_from, d_to = date.fromisoformat(from_date), date.fromisoformat(to_date)
        _check_trend_range(d_from, d_to)
        plan = build_department_trend(department, d_from, d_to)
    return {"endpoint": endpoint, "collection": plan.collection, "explain": _json_safe(explain_command(plan))}
