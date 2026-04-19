---
name: top_customer_concentration
version: 1.0.0
display_name: Top Customer Concentration
description: Reports the top N customers by ARR and their share of total ARR as of a specific date. Point-in-time safe uses date-range logic, not current status.
domain: revenue
inputs:
  - name: top_n
    description: "Number of top customers to return. Default: 10. 'top 5' = 5, 'top 20' = 20, 'top 50' = 50."
    type: integer
    default: 10
  - name: as_of_date
    description: "Snapshot date for active subscriptions (YYYY-MM-DD). 'in 2025' or 'end of 2025' = '2025-12-31', 'Q3 2025' = '2025-09-30'. Default: today."
    type: date
    default: ""
output_format: bar_chart_with_summary
test_prompts:
  - "top 10 customers by ARR"
  - "who are our biggest customers"
  - "customer concentration risk"
  - "revenue concentration analysis"
  - "show me our largest accounts"
  - "top customers by revenue"
  - "biggest accounts by ARR"
  - "what percent of revenue is top 10"
  - "customer concentration"
  - "largest customers"
  - "top 10 customers as of end of 2025"
  - "biggest accounts in Q3 2024"
sql_template: |
  WITH params AS (
    SELECT COALESCE(
      TRY_CAST(NULLIF('{as_of_date}', '') AS DATE),
      CURRENT_DATE
    ) AS snap_dt
  ),
  customer_arr AS (
    SELECT
      c.customer_id,
      c.company_name,
      c.segment,
      ROUND(SUM(s.mrr) * 12, 0) AS arr
    FROM customers c
    JOIN subscriptions s ON c.customer_id = s.customer_id, params p
    WHERE s.start_date <= p.snap_dt
      AND (s.end_date IS NULL OR s.end_date >= p.snap_dt)
      AND s.mrr > 0
    GROUP BY c.customer_id, c.company_name, c.segment
  ),
  total AS (
    SELECT SUM(arr) AS total_arr FROM customer_arr
  )
  SELECT
    ca.company_name,
    ca.segment,
    ca.arr,
    ROUND(ca.arr / t.total_arr * 100, 1) AS pct_of_total
  FROM customer_arr ca, total t
  ORDER BY ca.arr DESC
  LIMIT {top_n};
---

# Top Customer Concentration

Investors, board members, and risk committees all want this number. If the top 10 customers represent more than ~30% of ARR, the business has meaningful concentration risk.

## ARR Definition — Read This Before Trusting the Number

This skill defines a customer's ARR as **MRR × 12** summed across all their subscriptions that were active on `as_of_date`, where "active" means:

- `start_date ≤ as_of_date`
- `end_date IS NULL OR end_date ≥ as_of_date`
- `mrr > 0`

**What this definition includes:**
- All products with MRR > 0 active on the snapshot date (multi-product customers show combined ARR)
- Gross ARR — discounts assumed to be already netted into the MRR field

**What this definition explicitly excludes:**
- Subscriptions with `mrr = 0` (trials, POCs, internal accounts)
- Customers with no active subscriptions on `as_of_date` (historical churns are invisible here)
- Usage-based revenue above committed base (not modeled)

**Note on ranking stability:** Customer ranking can shift between snapshot dates as contracts expand, contract, or churn. Always state the `as_of_date` when citing this number.

**If your company's definition differs — fork this skill.** Common overrides: net ARR after discounts, contracted ARR only (exclude month-to-month), exclude pending renewals.

## Point-in-Time Guarantee

Uses date-range logic at `as_of_date`, not `status = 'active'` (a current-state field). Querying with `as_of_date = '2024-09-30'` returns the concentration that was true on September 30, 2024, provided the warehouse data has not been retroactively edited. For SOX-grade reproducibility, immutable warehouse snapshots at close dates are required — outside this skill's scope.

## Commentary guidance

1. State the share of ARR represented by the top N — that is the headline number
2. Always mention the snapshot date using a full date (e.g. "as of September 30, 2024")
3. Flag if any single customer is >5% of ARR (single-customer concentration risk)
4. Note the segment mix — all-Enterprise is expected; SMB-heavy top-10 is unusual and worth flagging
5. Do not speculate on churn risk for named customers — out of scope

## Edge cases

- Tied ARR amounts: deterministic ordering by customer_id as tiebreaker
- Customer names are synthetic in v0.1; real customers appear once warehouse is connected
