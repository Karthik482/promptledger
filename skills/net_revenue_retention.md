---
name: net_revenue_retention
version: 1.0.0
display_name: Net Revenue Retention (NRR)
description: Calculates Net Revenue Retention by customer segment over a configurable window. Point-in-time safe uses date-range logic, not current status. Supports lookback days or explicit start/end dates.
domain: retention
inputs:
  - name: start_date
    description: "Explicit cohort start date (YYYY-MM-DD). When provided, overrides lookback_days. 'Jan 11 2022' = '2022-01-11'. Leave blank to derive from end_date minus lookback_days."
    type: date
    default: ""
  - name: end_date
    description: "End date for the NRR window (YYYY-MM-DD). 'in 2025' or 'end of 2025' = '2025-12-31', 'Q2 2025' = '2025-06-30', 'Dec 15 2025' = '2025-12-15'. Default: today."
    type: date
    default: ""
  - name: lookback_days
    description: "Days to look back from end_date when start_date is not given. Default: 365. '6 months' = 180, '2 years' = 730. Ignored when start_date is provided."
    type: integer
    default: 365
output_format: bar_chart_with_summary
test_prompts:
  - "what is our NRR by segment"
  - "net revenue retention breakdown"
  - "show me NRR by customer tier"
  - "how is retention trending by segment"
  - "trailing twelve month NRR"
  - "are we expanding or churning by segment"
  - "NRR last 12 months"
  - "retention rate by segment"
  - "which segment has best NRR"
  - "is our NRR above 100"
  - "last 6 months NRR in 2025"
  - "NRR between Jan 2022 and Dec 2025"
  - "NRR for full year 2024"
  - "6 month NRR ending December 2025"
sql_template: |
  WITH params AS (
    SELECT
      COALESCE(
        TRY_CAST(NULLIF('{end_date}', '') AS DATE),
        CURRENT_DATE
      ) AS end_dt,
      CASE
        WHEN NULLIF('{start_date}', '') IS NOT NULL
          THEN TRY_CAST('{start_date}' AS DATE)
        ELSE COALESCE(
          TRY_CAST(NULLIF('{end_date}', '') AS DATE),
          CURRENT_DATE
        ) - INTERVAL '{lookback_days} days'
      END AS start_dt
  ),
  cohort AS (
    SELECT DISTINCT c.customer_id, c.segment
    FROM customers c
    JOIN subscriptions s ON c.customer_id = s.customer_id, params p
    WHERE s.start_date <= p.start_dt
      AND (s.end_date IS NULL OR s.end_date >= p.start_dt)
      AND s.mrr > 0
  ),
  starting_arr AS (
    SELECT
      c.segment,
      SUM(s.mrr) * 12 AS start_arr
    FROM cohort c
    JOIN subscriptions s ON c.customer_id = s.customer_id, params p
    WHERE s.start_date <= p.start_dt
      AND (s.end_date IS NULL OR s.end_date >= p.start_dt)
      AND s.mrr > 0
    GROUP BY c.segment
  ),
  ending_arr AS (
    SELECT
      c.segment,
      SUM(s.mrr) * 12 AS end_arr
    FROM cohort c
    JOIN subscriptions s ON c.customer_id = s.customer_id, params p
    WHERE s.start_date <= p.end_dt
      AND (s.end_date IS NULL OR s.end_date >= p.end_dt)
      AND s.mrr > 0
    GROUP BY c.segment
  )
  SELECT
    s.segment,
    ROUND(s.start_arr, 0) AS starting_arr,
    ROUND(COALESCE(e.end_arr, 0), 0) AS ending_arr,
    ROUND(COALESCE(e.end_arr, 0) / NULLIF(s.start_arr, 0) * 100, 1) AS nrr_pct
  FROM starting_arr s
  LEFT JOIN ending_arr e ON s.segment = e.segment
  ORDER BY nrr_pct DESC;
---

# Net Revenue Retention

NRR is the most-watched SaaS metric after ARR itself. Best-in-class is >120%; healthy is 100-120%; <100% means the cohort is shrinking.

## NRR Definition — Read This Before Trusting the Number

This skill defines NRR as **(ending cohort ARR / starting cohort ARR) × 100**, where:

**Cohort:** customers with at least one subscription with `mrr > 0` active on `start_dt`. New logos who joined after `start_dt` are excluded from both numerator and denominator — this is a retention metric, not a growth metric.

**Starting ARR:** MRR × 12 for cohort subscriptions active on `start_dt`.

**Ending ARR:** MRR × 12 for cohort subscriptions active on `end_dt`. Includes expansion (upsells, seat additions) this is **Net** NRR, not Gross NRR. A churned cohort customer contributes $0 to ending ARR.

**What this definition includes:**
- Expansion revenue from existing cohort customers (NRR > 100% territory)
- Contraction (downgrades) — reduces ending ARR
- Full churn — contributing $0 to ending ARR
- **Re-activations** — a cohort customer who churned mid-period and re-subscribed by `end_dt` will show ARR in ending_arr. This skill treats re-activation as indistinguishable from continuous subscription. If your definition excludes re-activations from NRR, fork this skill.

**What this definition explicitly excludes:**
- New logos acquired after `start_dt` (by definition)
- `mrr = 0` subscriptions (trials, POCs, internal)
- Usage-based revenue above committed base (not modeled)
- Pending renewals past end_date (excluded once end_date < end_dt)

**Cohort edge case — lapsed and re-subscribed customers:** If a customer had a subscription active on `start_dt`, that subscription later ended, and they started a new subscription after `start_dt`, they are correctly in the cohort (they had active ARR on `start_dt`). Their ending ARR is their new subscription's MRR × 12. This is the correct behavior under the "active on start_dt" cohort definition, but some finance teams exclude re-activations entirely. Document your company's choice.

**If your company's definition differs — fork this skill.** Common overrides: Gross NRR (exclude expansion), trailing-3-month usage for variable revenue, committed ARR only (exclude month-to-month).

## Point-in-Time Guarantee

Uses date-range logic (`start_date ≤ dt AND end_date ≥ dt`) at both `start_dt` and `end_dt`, not `status = 'active'`. Historical NRR numbers are reproducible as long as the warehouse data has not been retroactively edited. Same SOX caveat as other skills: immutable warehouse snapshots are required for audit-grade reproducibility.

## Commentary guidance

1. State the headline: which segments are above/below 100%
2. Explain what NRR means in plain English for non-finance readers
3. Always reference the exact time window using full dates (e.g. "July 1 to December 31, 2025")
4. Flag any segment <90% as a serious retention problem
5. NRR > 100% means expansion is outpacing churn in that cohort — name that explicitly
6. Avoid recommending tactical actions — that's the analyst's job, not the skill's

## Edge cases

- Segments with no cohort at start_dt return NULL — exclude from chart
- NRR > 200% is real if expansion was large — do not cap it
- When start_date and end_date are both provided, lookback_days is ignored entirely
