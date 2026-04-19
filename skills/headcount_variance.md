---
name: headcount_variance
version: 1.0.0
display_name: Headcount Variance vs Plan
description: Compares actual headcount to plan headcount by department for a specified month. Point-in-time safe uses the headcount snapshot table.
domain: people
inputs:
  - name: snapshot_month
    description: "Month to analyze as YYYY-MM-01 (first of the month). Empty string = most recent. Examples: 'March 2025' = '2025-03-01', 'last month' = '2026-03-01', 'end of 2025' = '2025-12-01'."
    type: date
    default: ""
output_format: bar_chart_with_summary
test_prompts:
  - "headcount variance vs plan"
  - "are we under or over plan on hiring"
  - "headcount by department"
  - "where are we behind on hiring"
  - "actual vs plan headcount"
  - "which departments are understaffed"
  - "hiring gap by team"
  - "headcount snapshot"
  - "are we at plan on people"
  - "show me people variance"
sql_template: |
  WITH target_date AS (
    SELECT COALESCE(
      TRY_CAST(NULLIF('{snapshot_month}', '') AS DATE),
      (SELECT MAX(snapshot_date) FROM headcount)
    ) AS dt
  )
  SELECT
    h.department,
    h.actual_headcount,
    h.plan_headcount,
    h.actual_headcount - h.plan_headcount AS variance,
    ROUND(
      (h.actual_headcount - h.plan_headcount)::DECIMAL
      / NULLIF(h.plan_headcount, 0) * 100, 1
    ) AS variance_pct
  FROM headcount h
  JOIN target_date t ON h.snapshot_date = t.dt
  ORDER BY variance_pct ASC;
---

# Headcount Variance vs Plan

The single most-asked question in any monthly business review. CFOs use this to decide whether to greenlight new reqs or pull back.

## Headcount Definition — Read This Before Trusting the Number

This skill compares `actual_headcount` vs `plan_headcount` from the `headcount` table for a given `snapshot_date`.

**What "actual_headcount" means in this warehouse:**
- A monthly snapshot — a point-in-time count of employees on payroll as of the snapshot date
- Includes full-time employees only (contractors, consultants, and interns are not modeled in this warehouse unless explicitly added)
- Counts bodies, not FTEs — a 0.5 FTE contractor would count as 1 if included

**What "plan_headcount" means:**
- The approved headcount plan as entered into this warehouse — typically from an annual plan or re-forecast
- Plans may be stale if the company re-forecasted mid-year without updating the warehouse

**Point-in-Time Guarantee:** The `headcount` table stores monthly snapshots. Each row is immutable for its `snapshot_date` querying March 2025 today returns the same number as querying it in April 2025, because it is a pre-computed snapshot, not a live count. This is inherently SOX-friendly compared to skills that query live transactional data.

**Caveat:** If the `headcount` table is loaded by overwriting rows (UPSERT rather than INSERT), historical snapshots may have been silently corrected. Verify your ETL pattern if reproducibility is critical.

**If your company's definition differs fork this skill.** Common overrides: include contractors (add a separate table), FTE-weighted count, exclude leaves of absence.

## Commentary guidance

1. Lead with the most-understaffed department (largest negative variance %)
2. Always state the snapshot date using a full month name (e.g. "as of March 1, 2025")
3. Call out any department >10% over plan (potential cost concern)
4. If aggregate variance is within ±2%, say "tracking to plan overall"
5. Do not recommend specific hiring actions — that is a People Ops decision

## Edge cases

- Departments without plan numbers show NULL variance — exclude from chart
- Variance % above ±20% may indicate plan staleness — flag it for review
- If requested month has no snapshot, the query returns zero rows — surface this as "no snapshot available for that month"
