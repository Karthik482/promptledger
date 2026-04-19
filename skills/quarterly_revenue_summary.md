---
name: quarterly_revenue_summary
version: 1.0.0
display_name: Quarterly Revenue Summary
description: Reports total ARR and ARR by customer segment for any specified quarter or date range. Point-in-time safe numbers reflect what was true on period_end.
domain: revenue
inputs:
  - name: period_start
    description: "Start date of the period (YYYY-MM-DD). Q1=01-01, Q2=04-01, Q3=07-01, Q4=10-01. 'Jan 11 2022' = '2022-01-11', 'start of 2025' = '2025-01-01'. Default: start of most recent complete quarter."
    type: date
    default: "2026-01-01"
  - name: period_end
    description: "End date of the period (YYYY-MM-DD). Q1=03-31, Q2=06-30, Q3=09-30, Q4=12-31. 'Dec 15 2025' = '2025-12-15', 'end of 2024' = '2024-12-31'. Default: end of most recent complete quarter."
    type: date
    default: "2026-03-31"
output_format: bar_chart_with_summary
test_prompts:
  - "what's our Q3 ARR by segment"
  - "show me revenue by segment for last quarter"
  - "ARR breakdown by customer tier"
  - "quarterly revenue split"
  - "how much ARR did each segment contribute last quarter"
  - "Q3 revenue summary"
  - "give me the latest quarter ARR"
  - "ending ARR by segment"
  - "current quarter revenue by customer type"
  - "where is our ARR coming from this quarter"
  - "ARR between Jan 2022 and Dec 2025"
  - "revenue for full year 2024"
  - "ARR from Jan 11 2022 to Dec 15 2025"
sql_template: |
  WITH period_subs AS (
    SELECT s.customer_id, s.mrr, c.segment
    FROM subscriptions s
    JOIN customers c ON s.customer_id = c.customer_id
    WHERE s.start_date <= DATE '{period_end}'
      AND (s.end_date IS NULL OR s.end_date >= DATE '{period_start}')
      AND s.mrr > 0
  )
  SELECT
    segment,
    ROUND(SUM(mrr) * 12, 0) AS arr,
    COUNT(DISTINCT customer_id) AS customers
  FROM period_subs
  GROUP BY segment
  ORDER BY arr DESC;
---

# Quarterly Revenue Summary

This skill produces a one-glance view of where company ARR is concentrated at a specific point in time.

## ARR Definition — Read This Before Trusting the Number

This skill defines ARR as **MRR × 12** for all subscriptions that were active during the period, where "active" means:

- `start_date ≤ period_end` — the subscription existed by the close of the period
- `end_date IS NULL OR end_date ≥ period_start` — the subscription had not yet ended at the start of the period
- `mrr > 0` — zero-MRR rows (free trials, internal test accounts, $0 POCs) are excluded

**What this definition includes:**
- All products in the `subscriptions` table with MRR > 0
- Multi-year deals recognized ratably (MRR × 12 per period — no upfront recognition)
- Gross ARR — discounts are assumed to be already netted into the MRR field

**What this definition explicitly excludes:**
- Subscriptions with `mrr = 0` (free trials, POCs, internal seats)
- One-time fees or implementation charges (these should have `mrr = 0` in a correctly modeled warehouse)
- Usage-based / variable revenue above a committed base (not modeled in this warehouse)
- Pending renewals past their end_date (excluded once end_date < period_start)

**If your company's definition differs — fork this skill.** Common overrides: include trailing-3-month usage average, exclude pending-renewal churn risk, use net ARR after discounts.

## Point-in-Time Guarantee

Querying with `period_end = '2024-09-30'` returns the ARR that was true on that date, **provided the warehouse data has not been retroactively edited**. This skill does not use `status = 'active'` (a current-state field) — it uses date-range logic only, which is correct for historical queries.

**Residual risk:** If a subscription's `end_date` was back-filled after the quarter closed (e.g. a dispute resolution), the historical number will differ from what was reported at close. For SOX-grade reproducibility, warehouse snapshots (immutable copies of the subscriptions table at close date) are required — that is a data engineering concern outside this skill's scope.

## Commentary guidance

1. Lead with total ARR and the dominant segment
2. Always state the period clearly using full month names (e.g. "January 1 to March 31, 2025")
3. Call out concentration risk if a single segment is >60% of ARR
4. Note any segment with fewer than 10 customers (volatility risk)
5. Avoid making QoQ growth claims unless explicitly asked — this skill is point-in-time

## Edge cases

- If a segment has zero active customers in the period, omit it from the chart
- All amounts in USD; no FX conversion in v0.1
- For full-year queries: period_start = Jan 1, period_end = Dec 31 of that year
