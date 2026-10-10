# Employee Attendance & Analytics API

FastAPI + MongoDB implementation of the HROne assignment (contract v2.0). All application code is in `app/main.py`.

## Run

Requires Python 3.11+ and MongoDB 6.0+ (tested on 7.0).

```bash
python -m venv .venv
# Windows PowerShell:  .venv\Scripts\Activate.ps1        macOS/Linux:  source .venv/bin/activate
pip install -r requirements.txt

# connection details come from the environment (a local .env file is also read; real environment variables win)
export MONGO_URI="mongodb://localhost:27017"      # PowerShell:  $env:MONGO_URI = "mongodb://localhost:27017"
export MONGO_DB="attendance_db"                   # PowerShell:  $env:MONGO_DB = "attendance_db"

uvicorn app.main:app --port 8000
```

The app refuses to start if `MONGO_URI` or `MONGO_DB` is missing (nothing is hard-coded). All indexes are created
at start-up, idempotently, so there are no manual steps. `GET /health` answers 200 when MongoDB responds, else 503.
Interactive API docs: `http://localhost:8000/docs`.

## Status

Everything in the contract is implemented: employees, punch-in / punch-out, regularisation with audit history, the four
analytics endpoints (each one a MongoDB aggregation pipeline) and `/admin/explain/{endpoint}`.

## Things worth knowing

- **Instants** are stored as BSON datetimes and exchanged as integer epoch milliseconds; the Mongo client is
  timezone-aware (UTC), so there are no naive datetimes anywhere. Calendar dates and shift times are IST strings.
- **Where the contract leaves room, I chose:**
  - `moving_avg_7d` is the mean of the *published* (already half-up rounded, 4 decimals) `attendance_rate` values, as the
    contract words it, rounded half-up again. It is computed with exact whole-number arithmetic (ten-thousandths) inside the
    pipeline, because MongoDB's `$round` is banker's rounding. The other reading (unrounded rates) differs in the 4th decimal
    on a small share of rows.
  - Two simultaneous corrections of the same record: the loser gets `409` (optimistic compare-and-set).
  - Analytics read the stored `late_minutes` / `work_hours` / ... and do not exclude records dated before `joined_on`.
- `REVIEW.md` lists the defects found in the starter file; `DECISIONS.md` answers the five design questions.
