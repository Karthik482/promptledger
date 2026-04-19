"""
engine.py
---------
The PromptLedger engine: loads skills, routes user questions to the
right skill, runs the SQL, and produces a natural language summary.
"""
import os
import re
import yaml
import json
from datetime import date, datetime
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

import duckdb
import pandas as pd
from anthropic import Anthropic


SKILLS_DIR   = Path(__file__).parent / "skills"
HISTORY_DIR  = SKILLS_DIR / "_history"
ARCHIVE_DIR  = SKILLS_DIR / "_archive"
MODEL = "claude-sonnet-4-5"

# ============ SQL SAFETY ============

_DDL_RE        = re.compile(r"\b(DROP|ALTER|CREATE|DELETE|UPDATE|INSERT|TRUNCATE)\b", re.IGNORECASE)
_CROSS_JOIN_RE = re.compile(r"\bCROSS\s+JOIN\b", re.IGNORECASE)
_HAS_LIMIT_RE  = re.compile(r"\bLIMIT\s+\d+", re.IGNORECASE)

def preflight_sql(sql: str) -> Optional[str]:
    """Return an error string if SQL is unsafe, else None."""
    if _DDL_RE.search(sql):
        return "SQL contains DDL (DROP/ALTER/INSERT/etc.) — not allowed."
    if _CROSS_JOIN_RE.search(sql):
        return "SQL contains CROSS JOIN — could produce an enormous result set."
    return None

def _ensure_limit(sql: str, n: int = 100) -> str:
    """Inject LIMIT n if the query has none."""
    if not _HAS_LIMIT_RE.search(sql):
        return sql.rstrip("; \n") + f"\nLIMIT {n};"
    return sql

# ============ CONFIDENCE FLOOR ============

_TRICKY_TERMS = frozenset([
    "runway", "ltv", "cac", "burn", "cash flow", "margin", "ebitda",
    "working capital", "capex", "opex", "payback", "cogs", "gross profit",
    "unit economics", "irr", "npv", "roce", "roic", "rule of 40",
])

def _is_tricky(question: str) -> bool:
    q = question.lower()
    return any(t in q for t in _TRICKY_TERMS)


@dataclass
class Skill:
    name: str
    display_name: str
    description: str
    domain: str
    test_prompts: list
    sql_template: str
    body_markdown: str
    inputs: list = field(default_factory=list)
    version: str = "1.0.0"


@dataclass
class ValidationResult:
    valid: bool
    errors: list   # Tier 1 — block save
    warnings: list  # Tier 2 — visible but non-blocking
    info: list      # Tier 3 — soft nudges


def load_skills() -> dict[str, Skill]:
    """Load all skill files from the skills directory."""
    skills = {}
    for f in sorted(SKILLS_DIR.glob("*.md")):
        try:
            content = f.read_text(encoding="utf-8")
            parts = content.split("---", 2)
            if len(parts) < 3:
                continue
            meta = yaml.safe_load(parts[1])
            if not meta or not isinstance(meta, dict):
                continue
            if "name" not in meta or "sql_template" not in meta:
                continue
            body = parts[2].strip()
            skills[meta["name"]] = Skill(
                name=meta["name"],
                display_name=meta.get("display_name", meta["name"]),
                description=meta.get("description", ""),
                domain=meta.get("domain", "general"),
                test_prompts=meta.get("test_prompts", []),
                sql_template=meta["sql_template"],
                body_markdown=body,
                inputs=meta.get("inputs", []),
                version=str(meta.get("version", "1.0.0")),
            )
        except Exception:
            continue  # skip malformed files silently
    return skills


# ============ ROUTER ============

def build_router_prompt(skills: dict[str, Skill], user_question: str) -> str:
    """Build the prompt that asks Claude to pick the right skill."""
    skill_descriptions = "\n".join([
        f'- "{s.name}": {s.description}'
        for s in skills.values()
    ])
    return f"""You are a router for a finance analytics platform. Your job is to pick the single most appropriate skill to answer the user's question.

Available skills:
{skill_descriptions}

User question: "{user_question}"

Respond with ONLY a JSON object in this exact format, no other text:
{{"skill": "skill_name_here", "confidence": 0.0-1.0, "reasoning": "one sentence why"}}

If no skill is a good match, return {{"skill": null, "confidence": 0.0, "reasoning": "..."}}."""


def route_question(client: Anthropic, skills: dict[str, Skill], question: str) -> dict:
    """Send question to Claude, get back a routing decision."""
    prompt = build_router_prompt(skills, question)
    response = client.messages.create(
        model=MODEL,
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text.strip()
    # Strip markdown fences if present
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"skill": None, "confidence": 0.0, "reasoning": f"Parse error: {text[:100]}"}


# ============ RUNNER ============

def run_skill(con: duckdb.DuckDBPyConnection, skill: Skill, params: dict = None) -> pd.DataFrame:
    """Execute the skill's SQL, substituting any {param} placeholders, and return a DataFrame."""
    sql = skill.sql_template
    if params:
        sql = sql.format(**{k: str(v) for k, v in params.items()})
    return con.execute(sql).fetchdf()


# ============ SUMMARIZER ============

def build_summary_prompt(skill: Skill, df: pd.DataFrame, question: str) -> str:
    """Build the prompt that asks Claude to write a natural-language summary."""
    return f"""You are a finance analyst writing a one-paragraph executive summary.

The user asked: "{question}"

You ran the skill "{skill.display_name}", described as: {skill.description}

The skill's commentary guidance is:
{skill.body_markdown}

The query returned these rows:
{df.to_markdown(index=False)}

Write a 2-4 sentence summary that:
- Leads with the headline number
- Follows the commentary guidance above
- Is written for a non-technical CFO
- Uses concrete numbers from the data
- Always writes dates in full as "Month Day, Year" (e.g. "February 3, 2004", "December 15, 2025") — NEVER as numbers like 02-03-2004 or 2025-12-15, to avoid ambiguity across regions
- Does NOT recommend specific actions unless the guidance says to

Respond with ONLY the summary text, no preamble."""


def summarize_result(client: Anthropic, skill: Skill, df: pd.DataFrame, question: str) -> str:
    """Get Claude to write a natural-language summary of the SQL result."""
    if df.empty:
        return "No results returned for this query."
    prompt = build_summary_prompt(skill, df, question)
    response = client.messages.create(
        model=MODEL,
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()


# ============ END-TO-END ============

WAREHOUSE_SCHEMA = """
Tables in the DuckDB warehouse:

customers(customer_id VARCHAR, company_name VARCHAR, segment VARCHAR,
          industry VARCHAR, country VARCHAR, signup_date DATE, churned_date DATE)
  -- churned_date IS NULL means the customer is active

subscriptions(subscription_id VARCHAR, customer_id VARCHAR, product VARCHAR,
              mrr DECIMAL, start_date DATE, end_date DATE, status VARCHAR)
  -- status: 'active' | 'churned' | 'expired'
  -- product values: 'Core Platform', 'Analytics Add-on', 'API Access', 'Premium Support', 'Compliance Pack'

invoices(invoice_id VARCHAR, customer_id VARCHAR, subscription_id VARCHAR,
         amount DECIMAL, invoice_date DATE, due_date DATE, paid_date DATE, status VARCHAR)
  -- status: 'paid' | 'overdue' | 'pending'

headcount(snapshot_date DATE, department VARCHAR,
          actual_headcount INTEGER, plan_headcount INTEGER)
"""


# ============ PARAM EXTRACTOR ============

def _most_recent_quarter() -> tuple[str, str]:
    """Return (start, end) ISO dates for the most recent complete quarter."""
    today = date.today()
    q = (today.month - 1) // 3  # 0-3, current quarter index
    if q == 0:
        # We're in Q1 — most recent complete quarter is Q4 of prior year
        return (f"{today.year - 1}-10-01", f"{today.year - 1}-12-31")
    starts = ["01-01", "04-01", "07-01", "10-01"]
    ends   = ["03-31", "06-30", "09-30", "12-31"]
    return (f"{today.year}-{starts[q - 1]}", f"{today.year}-{ends[q - 1]}")


def extract_params(client: Anthropic, skill: Skill, question: str) -> dict:
    """Ask Claude to extract concrete parameter values from the user's question."""
    if not skill.inputs:
        return {}

    inputs_desc = "\n".join(
        f'- "{inp["name"]}" ({inp.get("type","string")}): {inp.get("description","")}  Default: {inp.get("default","")}'
        for inp in skill.inputs
    )
    mrq_start, mrq_end = _most_recent_quarter()
    today_str = date.today().isoformat()

    prompt = f"""Extract query parameters for the "{skill.display_name}" skill.

Parameters:
{inputs_desc}

User question: "{question}"

Context:
- Today: {today_str}
- Warehouse data spans 2024-01-01 to 2026-04-01
- Most recent complete quarter: {mrq_start} to {mrq_end}
- Quarter calendar: Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec

Date extraction rules — apply ALL of these:
1. Always output dates as YYYY-MM-DD (ISO 8601). Never output ambiguous formats.
2. Ambiguous formats like 02-03-2004 or 02/03/2004: assume MM-DD-YYYY (US order) UNLESS the day part exceeds 12, in which case it must be DD-MM-YYYY. So 02-03-2004 → 2004-02-03 (February 3). 13-02-2004 → 2004-02-13 (February 13).
3. "in YYYY" or "for YYYY" (year only, no month): end_date or as_of_date = YYYY-12-31, start_date = YYYY-01-01 (when a range is needed).
4. Quarter references: Q1=Jan 1–Mar 31, Q2=Apr 1–Jun 30, Q3=Jul 1–Sep 30, Q4=Oct 1–Dec 31. "Q3 2025" → start=2025-07-01, end=2025-09-30.
5. "full year YYYY" → start=YYYY-01-01, end=YYYY-12-31.
6. If no year stated, use the most recent complete occurrence.
7. For snapshot_month: return YYYY-MM-01 (first of the month), or "" for default.
8. For as_of_date or end_date with no explicit end: if question says "in 2025", set to 2025-12-31. If quarter, set to last day of that quarter.
9. Leave date params as "" when no date context is implied in the question.

Respond with ONLY a JSON object containing the parameter values. Use defaults for unmentioned params."""

    response = client.messages.create(
        model=MODEL,
        max_tokens=200,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip().rstrip("```").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {inp["name"]: inp.get("default", "") for inp in skill.inputs}


# ============ FALLBACK SQL GENERATOR ============

def generate_fallback_sql(
    client: Anthropic,
    question: str,
    schema: str = None,
    error_context: str = None,
) -> str:
    """Ask Claude to write SQL for a question that no skill matched."""
    schema_text = schema if schema is not None else WAREHOUSE_SCHEMA
    retry_block = f"\n\nPrevious attempt failed — fix this:\n{error_context}" if error_context else ""
    response = client.messages.create(
        model=MODEL,
        max_tokens=600,
        messages=[{"role": "user", "content": f"""You are a DuckDB SQL expert. Write a SQL query to answer the user's question.

{schema_text}

User question: "{question}"{retry_block}

Rules:
- Write valid DuckDB SQL only — no DDL (DROP/ALTER/CREATE/INSERT)
- No CROSS JOINs
- Always include LIMIT 100 or fewer
- Use clear column aliases
- Respond with ONLY the SQL query, no explanation or markdown fences

DuckDB date/time syntax — use EXACTLY these forms, nothing else:
- Add/subtract intervals: date_col + INTERVAL '1 month', date_col - INTERVAL '12 months', date_col + INTERVAL '1 year'
- DO NOT use DATE_ADD(), DATE_SUB(), DATEADD(), or INTERVAL N MONTH (without quotes) — those are MySQL/SQL Server syntax and will fail
- Truncate to period: DATE_TRUNC('month', date_col), DATE_TRUNC('year', date_col)
- Difference in days: date_part('day', end_date - start_date)
- Current date: CURRENT_DATE
- Cast string to date: TRY_CAST('2025-01-01' AS DATE)
- Extract year/month: EXTRACT(year FROM date_col), EXTRACT(month FROM date_col)"""}],
    )
    sql = response.content[0].text.strip()
    if sql.startswith("```"):
        sql = sql.split("```")[1]
        if sql.startswith("sql"):
            sql = sql[3:]
        sql = sql.strip().rstrip("```").strip()
    return _ensure_limit(sql)


@dataclass
class AskResult:
    question: str
    skill_name: Optional[str]
    skill_display: Optional[str]
    confidence: float
    routing_reason: str
    sql: Optional[str]
    df: Optional[pd.DataFrame]
    summary: Optional[str]
    error: Optional[str]
    latency_ms: int
    queried_at: datetime = field(default_factory=datetime.now)
    skill_version: Optional[str] = None
    is_fallback: bool = False
    declined: bool = False
    params: dict = field(default_factory=dict)


def ask(
    client: Anthropic,
    con: duckdb.DuckDBPyConnection,
    skills: dict[str, Skill],
    question: str,
    schema: str = None,
    skip_routing: bool = False,
) -> AskResult:
    """The full pipeline: route -> extract params -> run -> summarize.

    schema: override the schema string sent to the fallback SQL generator.
    skip_routing: if True, bypass skill routing and always use fallback SQL.
    """
    import time
    t0 = time.time()
    queried_at = datetime.now()
    try:
        skill_name = None
        routing = {"skill": None, "confidence": 0.0, "reasoning": "User data mode — AI-generated SQL"}
        if not skip_routing:
            routing = route_question(client, skills, question)
            skill_name = routing.get("skill")

        if not skill_name or skill_name not in skills:
            # Confidence floor: refuse tricky finance terms with low routing confidence
            if not skip_routing and routing.get("confidence", 0.0) < 0.3 and _is_tricky(question):
                return AskResult(
                    question=question,
                    skill_name=None, skill_display="Declined",
                    confidence=routing.get("confidence", 0.0),
                    routing_reason="This metric needs a governed skill — AI-generated SQL is too risky here.",
                    sql=None, df=None, summary=None, error=None,
                    latency_ms=int((time.time() - t0) * 1000),
                    queried_at=queried_at,
                    declined=True,
                )

            # Generate SQL, pre-flight it, auto-retry once on failure
            sql = generate_fallback_sql(client, question, schema=schema)
            safety_err = preflight_sql(sql)
            if safety_err:
                sql = generate_fallback_sql(client, question, schema=schema,
                                            error_context=f"Safety violation: {safety_err}")
            try:
                df = con.execute(sql).fetchdf()
            except Exception as exec_err:
                sql = generate_fallback_sql(client, question, schema=schema,
                                            error_context=str(exec_err))
                df = con.execute(sql).fetchdf()

            summary = (
                client.messages.create(
                    model=MODEL,
                    max_tokens=300,
                    messages=[{"role": "user", "content": (
                        f'Summarize the following data neutrally in 2-3 sentences. '
                        f'Do not editorialize. If numbers look suspicious or inconsistent, say so explicitly. '
                        f'Question: "{question}"\n\n'
                        f'{df.to_markdown(index=False)}\n\n'
                        f'Respond with ONLY the summary text.'
                    )}],
                ).content[0].text.strip()
            )
            return AskResult(
                question=question,
                skill_name=None,
                skill_display="AI-generated",
                confidence=routing.get("confidence", 0.0),
                routing_reason="Generated custom SQL to answer your question.",
                sql=sql,
                df=df,
                summary=summary,
                error=None,
                latency_ms=int((time.time() - t0) * 1000),
                queried_at=queried_at,
                is_fallback=True,
            )

        skill = skills[skill_name]
        params = extract_params(client, skill, question)
        df = run_skill(con, skill, params)
        summary = summarize_result(client, skill, df, question)

        rendered_sql = skill.sql_template
        if params:
            rendered_sql = skill.sql_template.format(**{k: str(v) for k, v in params.items()})

        return AskResult(
            question=question,
            skill_name=skill.name,
            skill_display=skill.display_name,
            confidence=routing.get("confidence", 0.0),
            routing_reason=routing.get("reasoning", ""),
            sql=rendered_sql,
            df=df,
            summary=summary,
            error=None,
            latency_ms=int((time.time() - t0) * 1000),
            queried_at=queried_at,
            skill_version=skill.version,
            params=params,
        )
    except Exception as e:
        return AskResult(
            question=question,
            skill_name=None, skill_display=None,
            confidence=0.0, routing_reason="",
            sql=None, df=None, summary=None,
            error=str(e),
            latency_ms=int((time.time() - t0) * 1000),
            queried_at=queried_at,
        )


# ============ SUGGESTED QUESTIONS ============

def generate_suggested_questions(client: Anthropic, schema: str) -> list[str]:
    """Given a schema description, return 5 questions a user could ask."""
    response = client.messages.create(
        model=MODEL,
        max_tokens=300,
        messages=[{"role": "user", "content": f"""Given this database schema, suggest 5 specific, concrete questions a business analyst might ask. Each question must be directly answerable with a SQL query against these tables.

{schema}

Return ONLY a JSON array of 5 question strings. No explanation, no fences."""}],
    )
    text = response.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip().rstrip("```").strip()
    try:
        qs = json.loads(text)
        return [q for q in qs if isinstance(q, str)][:5]
    except (json.JSONDecodeError, TypeError):
        return []


# ============ SKILL PROMOTION ============

def find_similar_skills(question: str, skills: dict[str, "Skill"]) -> list[str]:
    """Return display names of skills with significant word overlap with the question."""
    q_words = set(re.findall(r"\b\w{4,}\b", question.lower()))
    similar = []
    for s in skills.values():
        corpus = s.description + " " + " ".join(s.test_prompts)
        s_words = set(re.findall(r"\b\w{4,}\b", corpus.lower()))
        if q_words and len(q_words & s_words) / len(q_words) >= 0.4:
            similar.append(s.display_name)
    return similar


def promote_fallback_to_skill(
    client: Anthropic,
    question: str,
    sql: str,
    result_df: pd.DataFrame,
    schema: str,
) -> dict:
    """Ask Claude to draft a full skill file. Returns a dict of parsed fields."""
    example_path = SKILLS_DIR / "customer_churn_analysis.md"
    example = example_path.read_text(encoding="utf-8") if example_path.exists() else ""
    today = date.today().isoformat()
    sample = result_df.head(5).to_markdown(index=False) if not result_df.empty else "(no rows)"
    schema = schema if schema is not None else WAREHOUSE_SCHEMA

    response = client.messages.create(
        model=MODEL,
        max_tokens=2500,
        messages=[{"role": "user", "content": f"""Generate a PromptLedger skill file.

=== EXAMPLE (follow this exact YAML + Markdown format) ===
{example}
=== END EXAMPLE ===

=== NEW SKILL ===
Original question: "{question}"

SQL that worked:
{sql}

Schema:
{schema}

Result sample (first 5 rows):
{sample}

Today: {today}

Requirements:
- YAML frontmatter fields: name (snake_case), display_name, description (≤120 chars),
  domain (revenue/retention/people/product/other), inputs (parameterize hardcoded years/dates
  with {{param_name}}), output_format, created_by: fallback_promotion, created_at: {today},
  source_question, test_prompts (6-8 natural language variations)
- Markdown body sections: Logic, Commentary guidance, Edge cases, Provenance
- Provenance section: note this was auto-generated from a fallback query on {today}

Respond with ONLY the raw file content starting with ---"""}],
    )

    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith(("yaml", "markdown", "md")):
            raw = raw.split("\n", 1)[1]
        raw = raw.strip().rstrip("```").strip()

    parts = raw.split("---", 2)
    if len(parts) < 3:
        return {
            "name": "user_skill", "display_name": question[:50],
            "description": question[:100], "domain": "other",
            "inputs": [], "test_prompts": [question],
            "sql_template": sql, "body": "", "raw": raw,
            "source_question": question, "created_at": today,
        }

    try:
        meta = yaml.safe_load(parts[1]) or {}
    except yaml.YAMLError:
        meta = {}

    return {
        "name": meta.get("name", "user_skill"),
        "display_name": meta.get("display_name", question[:50]),
        "description": meta.get("description", ""),
        "domain": meta.get("domain", "other"),
        "inputs": meta.get("inputs", []),
        "test_prompts": meta.get("test_prompts", [question]),
        "sql_template": meta.get("sql_template") or sql,
        "body": parts[2].strip(),
        "raw": raw,
        "source_question": question,
        "created_at": today,
        "created_by": "fallback_promotion",
    }


# ============ VALIDATION ============

_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


def validate_skill_fields(
    fields: dict,
    con: duckdb.DuckDBPyConnection = None,
    existing_skills: dict = None,
    old_skill: "Skill" = None,
) -> ValidationResult:
    """
    Tier 1 errors   → block save.
    Tier 2 warnings → show but allow save.
    Tier 3 info     → soft nudges, always shown.
    Never auto-fixes anything — shows the user exactly what is wrong.
    """
    errors, warnings, info = [], [], []

    # ── Tier 1: hard failures ────────────────────────────────────────────────
    name = fields.get("name", "").strip()
    if not name:
        errors.append("Skill ID is required.")
    elif not re.match(r"^[a-z][a-z0-9_]*$", name):
        errors.append(
            f"Skill ID must be snake_case (lowercase letters, digits, underscores, "
            f"starting with a letter). Got: '{name}'"
        )
    elif existing_skills and name in existing_skills:
        if old_skill is None or old_skill.name != name:
            errors.append(f"Skill ID '{name}' already exists — choose a different name.")

    if not fields.get("description", "").strip():
        errors.append("Description is required.")

    sql = fields.get("sql_template", "").strip()
    if not sql:
        errors.append("SQL template is required.")
    else:
        ddl_err = preflight_sql(sql)
        if ddl_err:
            errors.append(f"SQL safety check failed: {ddl_err}")
        elif con is not None:
            try:
                test_sql = re.sub(r"\{[^}]+\}", "1", sql)
                con.execute(f"EXPLAIN {test_sql}")
            except Exception as exc:
                errors.append(f"SQL syntax error: {exc}")

    test_prompts = [str(p).strip() for p in fields.get("test_prompts", []) if str(p).strip()]
    if len(test_prompts) < 3:
        errors.append(
            f"At least 3 test prompts are required (have {len(test_prompts)}). "
            "The router uses these to decide when to call this skill."
        )

    version = fields.get("version", "").strip()
    if version and not _SEMVER_RE.match(version):
        errors.append(f"Version must be semver (e.g. 1.0.0). Got: '{version}'")

    # SQL changed without bumping version → mandatory error
    if old_skill is not None and sql and version:
        if sql != old_skill.sql_template.strip() and version == old_skill.version:
            errors.append(
                f"SQL template changed but version is still {version}. "
                f"You must bump the version when editing SQL "
                f"(e.g. {version} → {_bump_patch(version)})."
            )

    # ── Tier 2: warnings ─────────────────────────────────────────────────────
    if sql:
        if re.search(r"\bSELECT\s+\*", sql, re.IGNORECASE):
            warnings.append("SQL uses SELECT * — risky if the schema changes.")
        if not _HAS_LIMIT_RE.search(sql) and "{" not in sql:
            warnings.append("SQL has no LIMIT clause — could return very large result sets.")
        if not re.search(r"\bWHERE\b", sql, re.IGNORECASE):
            warnings.append("SQL has no WHERE clause — full table scan on every run.")
        # Reference to tables not in warehouse schema
        warehouse_tables = {"customers", "subscriptions", "invoices", "headcount"}
        referenced = set(re.findall(r"\bFROM\s+(\w+)|\bJOIN\s+(\w+)", sql, re.IGNORECASE))
        flat = {t for pair in referenced for t in pair if t}
        unknown = flat - warehouse_tables - {"params", "cohort", "churned", "customer_arr",
                                              "total", "starting_arr", "ending_arr", "period_subs", "target_date"}
        if unknown:
            warnings.append(f"SQL references table(s) not in the demo warehouse: {', '.join(unknown)}")

    desc = fields.get("description", "")
    if desc and len(desc.strip()) < 20:
        warnings.append("Description is very short (< 20 chars) — consider expanding it.")

    if len(test_prompts) >= 3:
        unique_ratio = len(set(test_prompts)) / len(test_prompts)
        if unique_ratio < 0.7:
            warnings.append("Several test prompts look identical — the router may have trouble disambiguating this skill.")

    # ── Tier 3: soft nudges ───────────────────────────────────────────────────
    body = fields.get("body", "")
    if "## Commentary guidance" not in body:
        info.append("No '## Commentary guidance' section — summary quality will be reduced.")
    if "## Edge cases" not in body:
        info.append("No '## Edge cases' section — future maintainers will lack context.")

    inputs = fields.get("inputs", []) or []
    date_param_names = {"as_of_date", "start_date", "end_date",
                        "snapshot_month", "period_start", "period_end"}
    has_date = any(
        isinstance(inp, dict) and inp.get("name") in date_param_names
        for inp in inputs
    )
    if not has_date:
        info.append(
            "No date parameter — this skill always queries current state, not point-in-time. "
            "Finance queries should be reproducible at any past date."
        )

    return ValidationResult(valid=len(errors) == 0, errors=errors, warnings=warnings, info=info)


def _bump_patch(version: str) -> str:
    """1.2.3 → 1.2.4"""
    try:
        major, minor, patch = version.split(".")
        return f"{major}.{minor}.{int(patch) + 1}"
    except Exception:
        return version


# ============ SKILL FILE I/O ============

def _write_skill_content(name: str, fields: dict) -> str:
    """Serialize fields dict → .md file content string."""
    ordered_keys = [
        "name", "display_name", "version", "description", "domain", "inputs",
        "output_format", "requires_schema", "created_by", "created_at", "source_question", "test_prompts",
    ]
    meta = {k: fields[k] for k in ordered_keys if fields.get(k) is not None}
    meta["name"] = name
    if "version" not in meta:
        meta["version"] = "1.0.0"

    yaml_str = yaml.dump(meta, default_flow_style=False, allow_unicode=True, sort_keys=False)
    sql = fields["sql_template"]
    indented_sql = "\n".join("  " + line for line in sql.splitlines())
    body = fields.get("body", "")
    return f"---\n{yaml_str}sql_template: |\n{indented_sql}\n---\n\n{body}"


def save_skill_file(fields: dict, overwrite_name: str = None) -> str:
    """
    Write a skill .md file.
    overwrite_name: if provided, overwrite this existing skill (edit mode) after archiving it.
    Returns the saved skill name.
    """
    sql = fields.get("sql_template", "")
    safety = preflight_sql(sql)
    if safety:
        raise ValueError(safety)

    if overwrite_name:
        name = overwrite_name
        old_path = SKILLS_DIR / f"{name}.md"
        if old_path.exists():
            old_content = old_path.read_text(encoding="utf-8")
            try:
                old_meta = yaml.safe_load(old_content.split("---", 2)[1]) or {}
                old_ver = old_meta.get("version", "unknown")
            except Exception:
                old_ver = "unknown"
            HISTORY_DIR.mkdir(parents=True, exist_ok=True)
            archive = HISTORY_DIR / f"{name}_v{old_ver}.md"
            if not archive.exists():
                archive.write_text(old_content, encoding="utf-8")
    else:
        name = re.sub(r"[^a-z0-9]+", "_", fields.get("name", "user_skill").lower()).strip("_") or "user_skill"
        base, i = name, 1
        while (SKILLS_DIR / f"{name}.md").exists():
            name = f"{base}_{i}"
            i += 1

    content = _write_skill_content(name, fields)
    (SKILLS_DIR / f"{name}.md").write_text(content, encoding="utf-8")
    return name


def soft_delete_skill(name: str) -> None:
    """Move a skill file to skills/_archive/ instead of deleting it."""
    src = SKILLS_DIR / f"{name}.md"
    if not src.exists():
        return
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    dst = ARCHIVE_DIR / f"{name}.md"
    src.rename(dst)


# ============ SKILL AUTHORING (FROM SQL) ============

def generate_skill_from_sql(
    client: Anthropic,
    sql: str,
    schema: str = None,
) -> dict:
    """Given a SQL query, ask Claude to draft full skill fields (prompts, description, body)."""
    example_path = SKILLS_DIR / "customer_churn_analysis.md"
    example = example_path.read_text(encoding="utf-8") if example_path.exists() else ""
    today = date.today().isoformat()
    schema_text = schema if schema is not None else WAREHOUSE_SCHEMA

    response = client.messages.create(
        model=MODEL,
        max_tokens=2500,
        messages=[{"role": "user", "content": f"""Generate a PromptLedger skill file for the SQL query below.

=== EXAMPLE SKILL FILE (follow this exact YAML + Markdown format) ===
{example}
=== END EXAMPLE ===

=== SQL QUERY TO TURN INTO A SKILL ===
{sql}

Schema:
{schema_text}

Today: {today}

Requirements:
- YAML frontmatter fields: name (snake_case), display_name, version: 1.0.0,
  description (≤120 chars), domain (revenue/retention/people/product/other),
  inputs (extract any hardcoded dates/numbers as {{param_name}} placeholders),
  output_format: bar_chart_with_summary, created_by: manual_authoring,
  created_at: {today}, test_prompts (6-8 natural language variations a user might type)
- Markdown body: ## Logic, ## ARR Definition (or relevant metric definition),
  ## Commentary guidance, ## Edge cases sections
- The sql_template field must use {{param_name}} for any values that should be parameterizable

Respond with ONLY the raw file content starting with ---"""}],
    )

    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith(("yaml", "markdown", "md")):
            raw = raw.split("\n", 1)[1]
        raw = raw.strip().rstrip("`").strip()

    parts = raw.split("---", 2)
    if len(parts) < 3:
        return {
            "name": "new_skill", "display_name": "New Skill", "version": "1.0.0",
            "description": "", "domain": "other", "inputs": [], "test_prompts": [],
            "sql_template": sql, "body": "", "raw": raw,
            "created_at": today, "created_by": "manual_authoring",
        }

    try:
        meta = yaml.safe_load(parts[1]) or {}
    except yaml.YAMLError:
        meta = {}

    return {
        "name": meta.get("name", "new_skill"),
        "display_name": meta.get("display_name", "New Skill"),
        "version": str(meta.get("version", "1.0.0")),
        "description": meta.get("description", ""),
        "domain": meta.get("domain", "other"),
        "inputs": meta.get("inputs", []),
        "test_prompts": meta.get("test_prompts", []),
        "sql_template": meta.get("sql_template") or sql,
        "body": parts[2].strip(),
        "raw": raw,
        "created_at": today,
        "created_by": "manual_authoring",
    }


# ============ DATA PROFILER ============

def _name_similarity(col1: str, col2: str, table1: str, table2: str) -> float:
    """Name-based FK likelihood score [0.0, 1.0] between two columns."""
    c1, c2 = col1.lower(), col2.lower()
    if c1 == c2:
        return 1.0
    # col_id in one table ↔ id or col in the other
    for fk_col, id_col, ref_table in [(c1, c2, table2), (c2, c1, table1)]:
        if fk_col.endswith("_id"):
            base = fk_col[:-3]
            if id_col == "id" and (ref_table.rstrip("s").startswith(base) or base.startswith(ref_table.rstrip("s"))):
                return 0.9
            if id_col in (base, f"{base}_id"):
                return 0.85
    # Token overlap
    t1 = set(re.split(r"[_\s]+", c1)) - {""}
    t2 = set(re.split(r"[_\s]+", c2)) - {""}
    if t1 and t2:
        overlap = len(t1 & t2) / len(t1 | t2)
        if overlap >= 0.5:
            return round(overlap * 0.7, 2)
    return 0.0


def profile_uploaded_data(
    user_dfs: dict,
    skills: dict,
    con: duckdb.DuckDBPyConnection = None,  # unused; kept for API compatibility
) -> dict:
    """
    Profile uploaded CSV tables.

    Returns:
      tables       — {tname: {rows, cols, pk_candidates}}
      relationships — [{from_table, from_col, to_table, to_col, name_score, overlap_pct, confidence}]
      skill_compat  — {skill_name: {status, issues, display_name}}
    """
    import itertools

    # ── Table summary + PK detection ─────────────────────────────────────
    tables = {}
    for tname, df in user_dfs.items():
        n = len(df)
        pk_candidates = []
        for col in df.columns:
            if n == 0:
                continue
            null_ratio = df[col].isna().sum() / n
            uniq_ratio = df[col].nunique(dropna=True) / n
            if null_ratio < 0.05 and uniq_ratio >= 0.95:
                pk_candidates.append(col)
        tables[tname] = {"rows": n, "cols": len(df.columns), "pk_candidates": pk_candidates}

    # ── FK candidate detection ────────────────────────────────────────────
    relationships = []
    tnames = list(user_dfs.keys())
    for t1, t2 in itertools.combinations(tnames, 2):
        df1, df2 = user_dfs[t1], user_dfs[t2]
        for col1 in df1.columns:
            for col2 in df2.columns:
                name_score = _name_similarity(col1, col2, t1, t2)
                if name_score < 0.5:
                    continue
                v1 = set(df1[col1].dropna().astype(str).unique())
                v2 = set(df2[col2].dropna().astype(str).unique())
                if not v1 or not v2:
                    continue
                overlap = len(v1 & v2) / len(v1 | v2)
                if overlap < 0.1 and name_score < 0.85:
                    continue
                confidence = round(name_score * 0.6 + overlap * 0.4, 2)
                if confidence >= 0.3:
                    relationships.append({
                        "from_table": t1, "from_col": col1,
                        "to_table": t2, "to_col": col2,
                        "name_score": round(name_score * 100, 0),
                        "overlap_pct": round(overlap * 100, 1),
                        "confidence": confidence,
                    })

    relationships.sort(key=lambda r: r["confidence"], reverse=True)

    # ── Skill compatibility — pure schema check, no SQL execution ────────
    # "incompatible" = missing table(s)  |  "partial" = missing column(s)
    # Runtime errors (type mismatches, bad data) are surfaced when the user
    # actually asks a question, not here.
    user_schema: dict[str, set] = {
        tname: {c.lower() for c in df.columns}
        for tname, df in user_dfs.items()
    }

    skill_compat = {}
    for sk_name, skill in skills.items():
        issues = []
        sql = re.sub(r"\{[^}]+\}", "placeholder", skill.sql_template)

        # CTE aliases — these are virtual names defined inside the query, not real tables
        cte_names: set[str] = {
            m.lower() for m in re.findall(r"\b(\w+)\s+AS\s*\(", sql, re.IGNORECASE)
        }

        # Tables referenced in FROM / JOIN clauses
        raw_refs = re.findall(r"\b(?:FROM|JOIN)\s+(\w+)", sql, re.IGNORECASE)
        required_tables = {t.lower() for t in raw_refs} - cte_names

        missing_tables = required_tables - set(user_schema.keys())
        if missing_tables:
            status = "incompatible"
            issues.append(f"Missing tables: {', '.join(sorted(missing_tables))}")
        else:
            # Column-level check: look for table.column references
            col_refs = re.findall(r"\b(\w+)\.(\w+)\b", sql)
            missing_cols = []
            seen = set()
            for tbl, col in col_refs:
                tbl_l, col_l = tbl.lower(), col.lower()
                key = (tbl_l, col_l)
                if key in seen or tbl_l not in user_schema:
                    continue
                seen.add(key)
                if col_l not in user_schema[tbl_l]:
                    missing_cols.append(f"{tbl}.{col}")

            if missing_cols:
                status = "partial"
                issues.append(f"Missing columns: {', '.join(missing_cols[:6])}")
            else:
                status = "compatible"

        skill_compat[sk_name] = {
            "status": status,
            "issues": issues,
            "display_name": skill.display_name,
        }

    return {"tables": tables, "relationships": relationships, "skill_compat": skill_compat}


if __name__ == "__main__":
    # Quick smoke test
    skills = load_skills()
    print(f"Loaded {len(skills)} skills:")
    for s in skills.values():
        print(f"  - {s.name} ({len(s.test_prompts)} test prompts)")
