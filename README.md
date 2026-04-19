# PromptLedger

> AI analytics layer for finance teams. Ask your warehouse anything in plain English; get charts, executive summaries, and the SQL it ran.

This is the v0.1 demo. It runs against a synthetic SaaS finance warehouse (DuckDB, in-memory) so you can try the full flow without connecting real data.

## What's in here

```
promptledger/
├── app.py                 Streamlit app (4 pages)
├── engine.py              Skill loader + router + runner + summarizer
├── seed_warehouse.py      Generates the synthetic SaaS warehouse
├── run_evals.py           Test harness — measures routing accuracy
├── requirements.txt
├── skills/                YAML + Markdown skill files
│   ├── quarterly_revenue_summary.md
│   ├── net_revenue_retention.md
│   ├── customer_churn_analysis.md
│   ├── headcount_variance.md
│   └── top_customer_concentration.md
└── evals/
    └── results.json       Most recent eval run
```

## Run locally

**1. Install:**
```bash
pip install -r requirements.txt
```

**2. Set your Anthropic API key:**
```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

**3. Run:**
```bash
streamlit run app.py
```

Open http://localhost:8501.

## Run the evals

To regenerate `evals/results.json` with real numbers:

```bash
python run_evals.py
```

This sends every test prompt through the router and measures routing accuracy, latency, and confidence. Costs about $0.03 per full run on Claude Sonnet.

## Deploy to Streamlit Community Cloud (free)

1. Push this repo to GitHub (public)
2. Go to https://share.streamlit.io → New app → connect your repo
3. Main file path: `app.py`
4. Settings → Secrets → add:
   ```toml
   ANTHROPIC_API_KEY = "sk-ant-..."
   ```
5. Deploy. You get a URL like `your-app-name.streamlit.app`.

## The 4 pages

- **Ask** — Type a finance question. Routes to a skill, runs SQL, returns chart + summary.
- **Skill Library** — Browse the 5 skill files. See test prompts and SQL.
- **Evals** — Routing accuracy across 50 test prompts. Per-skill breakdown.
- **Warehouse** — Inspect the underlying tables and stats.

## Architecture

```
User question
    │
    ▼
Router (Claude Sonnet)  ──►  picks best matching skill from library
    │
    ▼
SQL Runner (DuckDB)     ──►  executes the skill's SQL template
    │
    ▼
Summarizer (Claude)     ──►  writes executive summary using skill's guidance
    │
    ▼
UI (Streamlit + Plotly)
```

Every answer cites the skill name, confidence score, latency, and SQL. No hallucinated metrics — all logic lives in version-controlled skill files.

## What this demo proves

- AI-first dev pattern (skill authoring, eval-driven iteration)
- Semantic-layer thinking (metrics defined once, used everywhere)
- Production-quality SQL (CTEs, window-style cohort logic, correctness)
- Streamlit fluency (multi-page app, custom theme, cached resources)
- Routing infrastructure (LLM-as-router with measured accuracy)

## What's next (if this becomes a real product)

- BigQuery + Snowflake connectors (replace DuckDB)
- Auth + workspaces (Supabase)
- Slack `/ask` integration
- Scheduled reports
- Skill versioning + A/B testing
- Approval workflows for production skills
