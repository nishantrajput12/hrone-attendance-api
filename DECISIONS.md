# DECISIONS.md  (about 150-200 words in total - plain language, your own words)

1. **Indexes.** Which indexes did you create, and which query or guarantee does each one serve? Which one did you consider and reject?

   Six, at start-up. employees: emp_code (unique, so duplicates give 409), (department, emp_code) for the department list, joined_on for headcount. attendance_logs: (emp_code, date) unique, one record per day; (date desc, emp_code) for list order and month ranges; (status, date desc, emp_code) for the status filter. Rejected: an index on late_minutes; the month range already narrows the data.

2. **Punch-in race.** Two identical punch-in requests arrive at the same instant. Walk through exactly what happens in your code.

   Each request reads the employee, then tries to insert the day's record. The unique (emp_code, date) index lets MongoDB accept only one; the other gets a duplicate-key error, which becomes 409. No check-then-insert, so no race window.

3. **Ties.** Two employees are tied on late minutes at the cutoff of `limit`. What does your leaderboard return, and why?

   It returns everyone tied within the limit, so it can return more than limit rows. I rank first with $rank (1, 2, 2, 4), then keep rank <= limit. Sort-then-cut would drop one tied person arbitrarily.

4. **Headcount.** How does your department summary make sure employees with zero logs are counted?

   The pipeline starts from employees, filtered by joined_on up to month end, and only then looks up their logs. Someone with no logs gets an empty lookup but is still counted in headcount.

5. **One thing you would change** if this had to serve 100x the data.

   I would keep a pre-aggregated daily summary per employee and department, updated on each punch, so analytics read few rows instead of millions. Today the department summary does one lookup per employee, so it grows with headcount.
