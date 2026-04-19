---
name: customer_churn_analysis
version: 1.0.0
display_name: Gross Customer Churn Analysis
description: Reports GROSS logo churn and GROSS dollar churn over a configurable window. Does not subtract win-backs. Point-in-time safe uses churned_date, not current status.
domain: retention
inputs:
  - name: start_date
    description: "Explicit window start date (YYYY-MM-DD). When provided, overrides lookback_days. 'Jan 2022' = '2022-01-01', 'Jan 11 2022' = '2022-01-11'. Leave blank to derive from end_date minus lookback_days."
    type: date
    default: ""
  - name: end_date
    description: "Window end date (YYYY-MM-DD). 'in 2025' = '2025-12-31', 'Q3 2025' = '2025-09-30', 'Dec 15 2025' = '2025-12-15'. Default: today."
    type: date
    default: ""
  - name: lookback_days
    description: "Days to look back from end_date when start_date is not given. Default: 90. 'last quarter' = 90, 'last year' = 365, '6 months' = 180. Ignored when start_date is provided."
    type: integer
    default: 90
output_format: bar_chart_with_summary
test_prompts:
  - "show me churn last quarter"
  - "which customers churned recently"
  - "logo churn by segment"
  - "dollar churn last 90 days"
  - "are we losing customers"
  - "churn breakdown"
  - "recent customer cancellations"
  - "lost ARR last quarter"
  - "who churned this quarter"
  - "what's our churn rate"
  - "churn between Jan 2022 and Dec 2025"
  - "customers lost in 2024"
  - "churn for full year 2025"
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
  churned AS (
    SELECT
      c.segment,
      c.customer_id,
      c.churned_date,
      COALESCE(SUM(s.mrr) * 12, 0) AS lost_arr
    FROM customers c
    LEFT JOIN subscriptions s ON c.customer_id = s.customer_id
      AND s.mrr > 0, params p
    WHERE c.churned_date IS NOT NULL
      AND c.churned_date >= p.start_dt
      AND c.churned_date <= p.end_dt
    GROUP BY c.segment, c.customer_id, c.churned_date
  )
  SELECT
    segment,
    COUNT(DISTINCT customer_id) AS logos_churned,
    ROUND(SUM(lost_arr), 0) AS arr_lost
  FROM churned
  GROUP BY segment
  ORDER BY arr_lost DESC;
---

# Gross Customer Churn Analysis

Churn is the single most important early-warning metric for SaaS health. The window is user-configurable via date range or lookback days.

## Churn Definition — Read This Before Trusting the Number

**This skill returns GROSS churn.** It counts all departures. It does not subtract win-backs (churned customers who re-subscribed). It does not subtract recoveries. The CFO who asks "what's our churn?" likely wants net dollar churn make sure you're giving them what they mean.

### The four churn numbers finance actually uses:

| Metric | What it means | This skill? |
|---|---|---|
| Gross logo churn | All customers who cancelled | ✓ Yes |
| Gross dollar churn | ARR from all cancellations | ✓ Yes |
| Net logo churn | Gross logos − win-backs | ✗ Not modeled |
| Net dollar churn | Gross ARR lost − ARR from win-backs | ✗ Use NRR skill |

**Logo churn:** count of distinct `customer_id` values with `churned_date` in the window. Each customer counts once regardless of how many subscriptions they had.

**Dollar churn (lost ARR):** MRR × 12 summed across all subscriptions (`mrr > 0`) the customer ever held. This is an approximation — it reflects historical billing value, not necessarily the ARR recorded at the moment of churn.

**What this definition includes:**
- Full cancellations (`churned_date` is set) in the window
- All segments equally — no minimum ARR threshold

**What this definition explicitly excludes:**
- Downgrades / contractions (customer stays active but reduces MRR) — those appear in NRR, not here
- Win-backs: a customer who churned and re-subscribed within the window is still counted as churned logos; their new ARR does NOT offset the lost ARR here. Use the NRR skill for net expansion/contraction view.
- Subscriptions with `mrr = 0` (trials, POCs) — excluded from lost ARR
- Voluntary vs. involuntary churn (not modeled in this warehouse)
- Customers whose account was merged or acquired (treated as churn in this model)

**Dollar churn caveat:** Lost ARR is computed from current subscription records at query time, not the ARR value snapshotted when the customer churned. If subscriptions were retroactively edited, the lost ARR figure may differ from what was reported at the time.

**If your company's definition differs — fork this skill.** Common overrides: logo churn rate (÷ starting logo count), revenue churn rate (÷ starting ARR), contraction included in dollar churn, cohort-based churn only, net of win-backs.

## Point-in-Time Guarantee

Uses `churned_date` (an event timestamp) rather than current status, so the count of churned logos in a historical window is deterministic and will not change as new customers churn. Lost ARR carries the caveat above.

## Commentary guidance

1. Lead with total logos churned and total ARR lost
2. Always state the time window clearly using full month names (e.g. "January 1 to December 31, 2025")
3. Identify the most-affected segment by dollars — one Enterprise loss outweighs ten SMB losses
4. If SMB/Startup churn is high but dollars are small, name that explicitly so leadership doesn't overreact
5. Never recommend "save plays" or specific retention tactics — out of scope

## Edge cases

- Zero churn in the window: return "no churn detected" rather than an empty chart
- When start_date and end_date are both provided, lookback_days is ignored entirely
