"""
app.py — PromptLedger Streamlit demo
-------------------------------------
Pages:
  1. Ask          — Type a question, see routing + SQL + chart + summary
  2. Skill Library — Browse the skill library
  3. Evals        — See routing accuracy across the test suite
  4. Warehouse    — Inspect the demo warehouse tables
  5. Connect Data — Toggle demo vs user CSVs; upload + auto-detect dates

Run: streamlit run app.py
Requires: ANTHROPIC_API_KEY env var (or .streamlit/secrets.toml)
"""
import io
import os
import re
import json
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from seed_warehouse import build_warehouse, get_warehouse_stats
from engine import (
    load_skills, ask, generate_suggested_questions,
    validate_skill_fields, save_skill_file, soft_delete_skill,
    generate_skill_from_sql, profile_uploaded_data, WAREHOUSE_SCHEMA,
)


# ============ CONFIG ============
st.set_page_config(
    page_title="PromptLedger",
    page_icon="●",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============ STYLING ============
st.markdown("""
<style>
    .stApp { background: #0a0c0a; }
    .main .block-container { padding-top: 2rem; max-width: 1200px; }
    h1, h2, h3 { font-family: 'Georgia', serif; letter-spacing: -0.02em; }
    .stButton>button {
        background: #b8ff3c; color: #0a0c0a;
        border: none; font-weight: 600; border-radius: 4px;
    }
    .stButton>button:hover { background: #c9ff5c; color: #0a0c0a; }
    div[data-testid="stMetric"] {
        background: #11140f; border: 1px solid #1f231a;
        padding: 16px; border-radius: 8px;
    }
    div[data-testid="stMetricValue"] { color: #b8ff3c; font-family: 'Georgia', serif; }
    code { background: #11140f; color: #b8ff3c; padding: 2px 6px; border-radius: 3px; }
    .skill-card {
        background: #11140f; border: 1px solid #1f231a;
        padding: 20px; border-radius: 8px; margin-bottom: 12px;
    }
    .pass-badge {
        background: rgba(184,255,60,0.15); color: #b8ff3c;
        padding: 2px 8px; border-radius: 3px; font-family: monospace; font-size: 11px;
    }
    .fail-badge {
        background: rgba(255,92,92,0.15); color: #ff5c5c;
        padding: 2px 8px; border-radius: 3px; font-family: monospace; font-size: 11px;
    }
    .mode-demo {
        background: rgba(184,255,60,0.15); color: #b8ff3c;
        padding: 3px 10px; border-radius: 4px; font-family: monospace;
        font-size: 12px; font-weight: 700; display: inline-block;
    }
    .mode-mydata {
        background: rgba(60,160,255,0.15); color: #3ca0ff;
        padding: 3px 10px; border-radius: 4px; font-family: monospace;
        font-size: 12px; font-weight: 700; display: inline-block;
    }
    .date-cast-badge {
        background: rgba(184,255,60,0.10); color: #b8ff3c;
        padding: 1px 6px; border-radius: 3px; font-family: monospace; font-size: 11px;
    }
    /* Suggested question buttons — fixed height, scrollable text */
    [data-testid="stHorizontalBlock"] [data-testid="stButton"] > button {
        height: 80px;
        overflow-y: auto;
        white-space: normal;
        align-items: flex-start;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)


# ============ SESSION STATE ============
if "data_mode" not in st.session_state:
    st.session_state.data_mode = "demo"          # "demo" | "my_data"
if "user_dfs" not in st.session_state:
    st.session_state.user_dfs = {}               # table_name -> pd.DataFrame
if "user_date_cols" not in st.session_state:
    st.session_state.user_date_cols = {}         # table_name -> [col, ...]
if "user_question" not in st.session_state:
    st.session_state.user_question = ""
if "question_history" not in st.session_state:
    st.session_state.question_history = []   # list of {question, skill, is_fallback}
if "flagged" not in st.session_state:
    st.session_state.flagged = set()
if "suggested_qs" not in st.session_state:
    st.session_state.suggested_qs = []
if "current_page" not in st.session_state:
    st.session_state.current_page = "Ask"
if "editing_skill_name" not in st.session_state:
    st.session_state.editing_skill_name = None   # skill name being edited, or None
if "add_skill_draft" not in st.session_state:
    st.session_state.add_skill_draft = {}        # Claude-generated draft fields
if "add_skill_draft_id" not in st.session_state:
    st.session_state.add_skill_draft_id = None   # key to detect when draft changes
if "data_profile" not in st.session_state:
    st.session_state.data_profile = None          # result of profile_uploaded_data()
if "rel_decisions" not in st.session_state:
    st.session_state.rel_decisions = {}           # rel_id -> "accepted" | "rejected"


# ============ CACHED RESOURCES ============
@st.cache_resource
def get_warehouse():
    return build_warehouse()

@st.cache_resource(ttl=30)
def get_skills():
    return load_skills()

@st.cache_data
def get_eval_results():
    path = Path(__file__).parent / "evals" / "results.json"
    if path.exists():
        return json.loads(path.read_text())
    return None

def get_anthropic_client():
    from anthropic import Anthropic
    key = os.getenv("ANTHROPIC_API_KEY") or st.secrets.get("ANTHROPIC_API_KEY", None)
    if not key:
        return None
    return Anthropic(api_key=key)


# ============ USER DATA HELPERS ============

_DATE_PATTERNS = re.compile(
    r"(date|_at$|_on$|time|timestamp|created|updated|signup|start|end|"
    r"birth|expir|cancel|churn|closed|launch|published|activated)",
    re.IGNORECASE,
)

def _looks_like_date_col(col: str) -> bool:
    return bool(_DATE_PATTERNS.search(col))

def _auto_cast_dates(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Cast date-like columns to datetime64[ns]. Returns (df, cast_column_names).

    Stores as datetime64[ns] (not Python date objects) so DuckDB maps them
    to TIMESTAMP, which supports DATE comparisons via implicit casting.
    """
    cast = []
    for col in df.columns:
        if not _looks_like_date_col(col):
            continue
        dtype_str = str(df[col].dtype)
        # Skip columns that are already numeric or datetime
        if any(t in dtype_str for t in ("int", "float", "datetime")):
            continue
        converted = pd.to_datetime(df[col], errors="coerce")
        if converted.notna().mean() >= 0.5:
            df[col] = converted          # datetime64[ns] — DuckDB maps this to TIMESTAMP
            cast.append(col)
    return df, cast

def _dtype_label(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "INTEGER"
    if pd.api.types.is_float_dtype(series):
        return "DECIMAL"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    if series.dropna().apply(lambda x: isinstance(x, date)).any():
        return "DATE"
    return "VARCHAR"

def build_user_schema(user_dfs: dict, user_date_cols: dict) -> str:
    """Build a schema description string for the fallback SQL generator."""
    if not user_dfs:
        return ""
    parts = ["Tables in the uploaded database:\n"]
    for tname, df in user_dfs.items():
        col_defs = []
        for col in df.columns:
            label = _dtype_label(df[col])
            col_defs.append(f"{col} {label}")
        parts.append(f"{tname}({', '.join(col_defs)})")
        date_cols = user_date_cols.get(tname, [])
        if date_cols:
            parts.append(f"  -- date columns: {', '.join(date_cols)}")
        # Add a few sample values to help Claude understand domain
        sample = df.head(2).to_dict(orient="records")
        parts.append(f"  -- sample rows: {json.dumps(sample, default=str)[:300]}")
    return "\n".join(parts)

def get_user_con() -> duckdb.DuckDBPyConnection:
    """Build a DuckDB connection from stored user DataFrames.

    Detects date columns by name pattern and casts them to TIMESTAMP.
    Using TIMESTAMP (not DATE) because DuckDB's interval arithmetic
    (e.g. CURRENT_DATE - INTERVAL '90 days') returns TIMESTAMP, so both
    sides of skill SQL comparisons end up as the same type.
    Works regardless of how dates are stored in the DataFrame (strings,
    Python date objects, or datetime64) — TRY_CAST handles all of them.
    """
    con = duckdb.connect(":memory:")
    for tname, df in st.session_state.user_dfs.items():
        con.register(f"_raw_{tname}", df)
        col_exprs = []
        for col in df.columns:
            if _looks_like_date_col(col):
                col_exprs.append(f'TRY_CAST("{col}" AS TIMESTAMP) AS "{col}"')
            else:
                col_exprs.append(f'"{col}"')
        con.execute(
            f'CREATE TABLE "{tname}" AS SELECT {", ".join(col_exprs)} FROM "_raw_{tname}"'
        )
    return con


# ============ SIDEBAR ============
with st.sidebar:
    st.markdown("### ● PromptLedger")
    st.caption("v0.1 Beta — Finance Analytics Demo")

    # Mode badge
    if st.session_state.data_mode == "demo":
        st.markdown('<span class="mode-demo">● DEMO MODE</span>', unsafe_allow_html=True)
    else:
        n = len(st.session_state.user_dfs)
        label = f"{n} table{'s' if n != 1 else ''} loaded"
        st.markdown(f'<span class="mode-mydata">● MY DATA &nbsp;·&nbsp; {label}</span>', unsafe_allow_html=True)

    if st.button("↺ Reload Skills", use_container_width=True):
        get_skills.clear()
        st.rerun()

    st.divider()

    _pages = ["Ask", "Skill Library", "Evals", "Warehouse", "Connect Data"]
    # "Add Skill" is not a nav item — it's accessed via buttons in Skill Library.
    # While on Add Skill, keep Skill Library highlighted in the sidebar.
    _radio_default = st.session_state.current_page if st.session_state.current_page in _pages else "Skill Library"
    page = st.radio(
        "Navigate",
        _pages,
        index=_pages.index(_radio_default),
        label_visibility="collapsed",
    )
    # If the user clicked a different nav item, honour it (exits Add Skill)
    if page != _radio_default:
        st.session_state.current_page = page
        st.rerun()

    st.divider()
    if st.session_state.data_mode == "demo":
        st.caption("**Connected to:**")
        st.code("acme-corp.duckdb\n(synthetic SaaS data)", language=None)
        stats = get_warehouse_stats(get_warehouse())
        st.caption(f"📊 {stats['active_customers']} customers · ${float(stats['current_arr'])/1e6:.1f}M ARR")
    else:
        st.caption("**Connected to:**")
        st.code("user-upload.duckdb\n(your CSV data)", language=None)
        if st.session_state.user_dfs:
            total_rows = sum(len(df) for df in st.session_state.user_dfs.values())
            st.caption(f"📊 {len(st.session_state.user_dfs)} tables · {total_rows:,} rows")

    # Question history
    if st.session_state.question_history:
        st.divider()
        st.caption("**Recent questions**")
        for item in st.session_state.question_history[:20]:
            label = "⚡" if item["is_fallback"] else "●"
            if st.button(f"{label} {item['question'][:38]}", key=f"hist_{item['question'][:38]}", use_container_width=True):
                st.session_state.user_question = item["question"]
                st.rerun()


# ============ PAGE 1: ASK ============
if page == "Ask":
    st.title("Ask your warehouse anything.")

    # Mode banner
    if st.session_state.data_mode == "demo":
        st.caption("Routed to governed skills · synthetic SaaS data · "
                   '<span class="mode-demo">DEMO MODE</span>', unsafe_allow_html=True)
    else:
        if not st.session_state.user_dfs:
            st.warning("No data uploaded yet. Go to **Connect Data** and upload CSVs first.")
            st.stop()
        st.caption(
            f"AI-generated SQL against your data · "
            f'{len(st.session_state.user_dfs)} tables · '
            '<span class="mode-mydata">MY DATA</span>', unsafe_allow_html=True
        )
        if st.session_state.suggested_qs:
            st.markdown("**Suggested questions for your data:**")
            sq_cols = st.columns(len(st.session_state.suggested_qs))
            for i, q in enumerate(st.session_state.suggested_qs):
                if sq_cols[i].button(q, key=f"ask_sugg_{i}", use_container_width=True):
                    st.session_state.user_question = q

    # Sample questions (demo mode only)
    if st.session_state.data_mode == "demo":
        st.markdown("**Try one of these:**")
        cols = st.columns(5)
        sample_qs = [
            "Q3 ARR by segment",
            "NRR last 12 months",
            "show me churn last quarter",
            "headcount variance vs plan",
            "top 10 customers by ARR",
        ]
        for i, q in enumerate(sample_qs):
            if cols[i].button(q, key=f"sample_{i}", use_container_width=True):
                st.session_state.user_question = q

    question = st.text_input(
        "Ask a question",
        value=st.session_state.user_question,
        placeholder="e.g. what's our Q3 ARR by segment?",
        label_visibility="collapsed",
    )
    run_clicked = st.button("Ask →", type="primary")

    if run_clicked and question:
        client = get_anthropic_client()
        if not client:
            st.error("⚠️ ANTHROPIC_API_KEY not set.")
        else:
            skills = get_skills()
            is_my_data = st.session_state.data_mode == "my_data"

            if is_my_data:
                con = get_user_con()
                schema = build_user_schema(st.session_state.user_dfs, st.session_state.user_date_cols)
                # Only route to skills whose SQL runs on the uploaded schema.
                # If none are compatible, skip routing and go straight to fallback.
                compat_map = (st.session_state.data_profile or {}).get("skill_compat", {})
                active_skills = {
                    name: sk for name, sk in skills.items()
                    if compat_map.get(name, {}).get("status") == "compatible"
                }
                skip_routing = len(active_skills) == 0
            else:
                con = get_warehouse()
                schema = None
                active_skills = skills
                skip_routing = False

            with st.spinner("Routing → extracting parameters → running SQL → summarizing..."):
                result = ask(
                    client, con, active_skills, question,
                    schema=schema,
                    skip_routing=skip_routing,
                )

            if result.error:
                st.error(f"Error: {result.error}")

            elif result.declined:
                st.error(
                    "**This question needs a governed skill.**\n\n"
                    f"{result.routing_reason}\n\n"
                    "Consider adding a skill that covers this metric, or rephrase your question "
                    "to match an existing skill."
                )

            else:
                # ── Record history ──────────────────────────────────────────
                entry = {
                    "question": question,
                    "skill": result.skill_display or "AI-generated",
                    "is_fallback": result.is_fallback,
                }
                if entry not in st.session_state.question_history:
                    st.session_state.question_history.insert(0, entry)
                    st.session_state.question_history = st.session_state.question_history[:20]

                # ── Fallback card ────────────────────────────────────────────
                if result.is_fallback:
                    with st.container(border=True):
                        fc1, fc2 = st.columns([6, 1])
                        with fc1:
                            st.markdown("**⚡ AI-Generated Answer** — not a governed skill. Verify before using in reports.")
                        with fc2:
                            already_flagged = question in st.session_state.flagged
                            flag_label = "⚑ Flagged" if already_flagged else "⚑ Flag"
                            if st.button(flag_label, key="flag_btn", type="secondary"):
                                if already_flagged:
                                    st.session_state.flagged.discard(question)
                                else:
                                    st.session_state.flagged.add(question)
                                st.rerun()


                # ── Metrics row ──────────────────────────────────────────────
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Method" if result.is_fallback else "Skill", result.skill_display)
                if result.is_fallback:
                    m2.metric("Routing", "Skipped")
                else:
                    m2.metric("Confidence", f"{result.confidence*100:.0f}%")
                m3.metric("Latency", f"{result.latency_ms} ms")
                m4.metric("Rows", len(result.df) if result.df is not None else 0)
                m5.metric("Skill version", result.skill_version or "—")

                if not result.is_fallback:
                    st.caption(f"💡 **Routing reason:** {result.routing_reason}")
                if result.params:
                    param_str = "  ·  ".join(f"`{k}` = `{v}`" for k, v in result.params.items())
                    st.caption(f"⚙️ **Parameters:** {param_str}")
                st.divider()

                # ── Chart + table ────────────────────────────────────────────
                st.subheader("Result")
                if result.df is not None and not result.df.empty:
                    chart_df = result.df.copy()
                    str_cols = chart_df.select_dtypes(include=["object"]).columns.tolist()
                    num_cols = chart_df.select_dtypes(include=["number"]).columns.tolist()
                    if str_cols and num_cols:
                        fig = px.bar(
                            chart_df, x=str_cols[0], y=num_cols[0],
                            color_discrete_sequence=["#b8ff3c"], template="plotly_dark",
                        )
                        fig.update_layout(
                            plot_bgcolor="#11140f", paper_bgcolor="#11140f",
                            font_color="#e8e9e3", margin=dict(t=20, b=20, l=20, r=20), height=350,
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        if result.is_fallback:
                            st.caption("⚡ AI-generated · verify SQL before using in reports")
                    st.dataframe(chart_df, use_container_width=True, hide_index=True)
                    st.download_button(
                        label="Download CSV",
                        data=chart_df.to_csv(index=False).encode("utf-8"),
                        file_name="result.csv",
                        mime="text/csv",
                    )
                else:
                    st.info("No data returned.")

                if result.summary:
                    st.subheader("Summary")
                    st.markdown(f"> {result.summary}")

                # SQL expander — open by default for fallback so it's unmissable
                with st.expander("View SQL", expanded=result.is_fallback):
                    st.code(result.sql, language="sql")

                # Finance-grade provenance stamp — every number needs an as-of time
                queried_str = result.queried_at.strftime("%B %d, %Y %I:%M %p")
                drift_note = " · Historical numbers may drift if warehouse data was retroactively edited." if result.params else ""
                st.caption(f"🕐 Data queried on {queried_str}{drift_note}")

    elif run_clicked and not question:
        st.warning("Type a question first.")


# ============ PAGE 2: SKILL LIBRARY ============
elif page == "Skill Library":
    st.title("Skill Library")
    st.caption("Each skill is a YAML + Markdown file. Forkable, versionable, auditable.")

    skills = get_skills()
    h1, h2 = st.columns([4, 1])
    h1.metric("Skills loaded", len(skills))
    if h2.button("+ Add Skill", type="primary", use_container_width=True):
        st.session_state.editing_skill_name = None
        st.session_state.add_skill_draft = {}
        st.session_state.add_skill_draft_id = None
        st.session_state.current_page = "Add Skill"
        st.rerun()

    for skill_name, skill in skills.items():
        label = f"**{skill.display_name}** `v{skill.version}` — {skill.description[:75]}..."
        with st.expander(label):
            c1, c2 = st.columns([2, 1])
            with c1:
                st.markdown(f"**Domain:** `{skill.domain}` &nbsp;·&nbsp; **Version:** `{skill.version}`")
                st.markdown(f"**Description:** {skill.description}")
                if skill.inputs:
                    st.markdown("**Parameters:**")
                    for inp in skill.inputs:
                        st.markdown(f"- `{inp['name']}` ({inp.get('type','string')}) — {inp.get('description','')}")
                st.markdown("**Test prompts:**")
                for p in skill.test_prompts:
                    st.markdown(f"- _{p}_")
            with c2:
                st.markdown("**Skill body:**")
                st.markdown(skill.body_markdown[:600] + "...")
            st.markdown("**SQL template:**")
            st.code(skill.sql_template, language="sql")

            st.divider()
            btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 4])
            if btn_col1.button("Edit", key=f"edit_{skill_name}", type="primary"):
                st.session_state.editing_skill_name = skill_name
                st.session_state.add_skill_draft = {}
                st.session_state.add_skill_draft_id = None
                st.session_state.current_page = "Add Skill"
                st.rerun()
            if btn_col2.button("Archive", key=f"del_{skill_name}", type="secondary"):
                soft_delete_skill(skill_name)
                get_skills.clear()
                st.rerun()
            btn_col3.caption("Archive moves the file to skills/_archive/ — not permanently deleted.")


# ============ PAGE 3: ADD SKILL ============
elif page == "Add Skill":
    editing_name = st.session_state.editing_skill_name
    skills = get_skills()

    if editing_name:
        st.title(f"Edit Skill: {editing_name}")
        st.caption("Editing an existing skill. Changing SQL requires a version bump — the old version is auto-archived.")
    else:
        st.title("Add Skill")
        st.caption("Author a new governed skill. Intentional authoring — you decide the definition, test prompts, and SQL.")

    # ── Mode selector (only for new skills) ───────────────────────────────
    if not editing_name:
        mode = st.radio(
            "Authoring mode",
            ["From Scratch", "Fork Existing Skill", "From SQL"],
            horizontal=True,
        )
    else:
        mode = "From Scratch"  # editing always uses form directly

    client = get_anthropic_client()

    # ── Pre-fill draft based on mode ─────────────────────────────────────
    draft_id = f"{mode}_{editing_name or ''}"

    if st.session_state.add_skill_draft_id != draft_id:
        if editing_name and editing_name in skills:
            sk = skills[editing_name]
            st.session_state.add_skill_draft = {
                "name": sk.name,
                "display_name": sk.display_name,
                "version": sk.version,
                "description": sk.description,
                "domain": sk.domain,
                "inputs": sk.inputs,
                "test_prompts": sk.test_prompts,
                "sql_template": sk.sql_template,
                "body": sk.body_markdown,
            }
        elif mode == "From Scratch":
            st.session_state.add_skill_draft = {
                "name": "", "display_name": "", "version": "1.0.0",
                "description": "", "domain": "revenue", "inputs": [],
                "test_prompts": [], "sql_template": "", "body": "",
            }
        # Fork and From SQL drafts are set later by buttons
        st.session_state.add_skill_draft_id = draft_id

    # ── Fork mode: pick source skill ─────────────────────────────────────
    if mode == "Fork Existing Skill":
        fork_src = st.selectbox("Fork from", list(skills.keys()), format_func=lambda n: skills[n].display_name)
        if st.button("Load for forking"):
            sk = skills[fork_src]
            st.session_state.add_skill_draft = {
                "name": f"{sk.name}_fork",
                "display_name": f"{sk.display_name} (Fork)",
                "version": "1.0.0",
                "description": sk.description,
                "domain": sk.domain,
                "inputs": sk.inputs,
                "test_prompts": sk.test_prompts,
                "sql_template": sk.sql_template,
                "body": sk.body_markdown,
            }
            st.session_state.add_skill_draft_id = draft_id + "_loaded"
            st.rerun()

    # ── From SQL mode: paste SQL, Claude fills the rest ──────────────────
    if mode == "From SQL":
        st.markdown("**Paste a SQL query that already works.** Claude will generate the skill name, description, test prompts, and commentary.")
        raw_sql = st.text_area("SQL query", height=180, placeholder="SELECT ...", key="from_sql_input")
        if st.button("Generate skill from SQL →", type="primary", disabled=not raw_sql.strip()):
            if not client:
                st.error("ANTHROPIC_API_KEY not set.")
            else:
                schema = build_user_schema(st.session_state.user_dfs, st.session_state.user_date_cols) \
                    if st.session_state.data_mode == "my_data" else WAREHOUSE_SCHEMA
                with st.spinner("Claude is drafting skill fields from your SQL…"):
                    draft = generate_skill_from_sql(client, raw_sql.strip(), schema)
                st.session_state.add_skill_draft = draft
                st.session_state.add_skill_draft_id = draft_id + "_generated"
                st.rerun()

    st.divider()
    d = st.session_state.add_skill_draft
    if not d and mode in ("Fork Existing Skill", "From SQL"):
        st.info("Use the controls above to load a starting point, then fill out the form below.")
        st.stop()

    # ── Pre-fill widget session state keys when draft changes ────────────
    _fid = st.session_state.add_skill_draft_id or ""
    _init_key = f"_sf_inited_{_fid}"
    if not st.session_state.get(_init_key):
        st.session_state["_sf_name"]     = d.get("name", "")
        st.session_state["_sf_display"]  = d.get("display_name", "")
        st.session_state["_sf_version"]  = d.get("version", "1.0.0")
        st.session_state["_sf_desc"]     = d.get("description", "")
        st.session_state["_sf_domain"]   = d.get("domain", "revenue")
        st.session_state["_sf_prompts"]  = "\n".join(d.get("test_prompts", []))
        st.session_state["_sf_sql"]      = d.get("sql_template", "")
        st.session_state["_sf_body"]     = d.get("body", "")
        st.session_state[_init_key]      = True

    # ── Form fields ───────────────────────────────────────────────────────
    st.subheader("Skill metadata")
    fc1, fc2, fc3 = st.columns([3, 3, 1])
    sf_name    = fc1.text_input("Skill ID (snake_case)", key="_sf_name", disabled=bool(editing_name))
    sf_display = fc2.text_input("Display name", key="_sf_display")
    sf_version = fc3.text_input("Version (semver)", key="_sf_version")

    if editing_name:
        st.caption("Skill ID is locked when editing. To rename a skill, archive it and create a new one.")

    sf_desc   = st.text_input("Description (≤120 chars)", key="_sf_desc")
    _DOMAINS  = ["revenue", "retention", "people", "product", "other"]
    _dom_idx  = _DOMAINS.index(st.session_state["_sf_domain"]) if st.session_state["_sf_domain"] in _DOMAINS else 0
    sf_domain = st.selectbox("Domain", _DOMAINS, index=_dom_idx, key="_sf_domain_sel")

    st.subheader("Test prompts")
    st.caption("One per line. Minimum 3. These teach the router when to use this skill — be specific and varied.")
    sf_prompts_raw = st.text_area("prompts", label_visibility="collapsed", key="_sf_prompts", height=140)

    st.subheader("SQL template")
    st.caption("Use {param_name} placeholders for any values users might vary (dates, N, segments). Never hardcode a year.")
    sf_sql = st.text_area("SQL", label_visibility="collapsed", key="_sf_sql", height=220)

    with st.expander("Skill body (Logic, Commentary guidance, Edge cases, Definition)"):
        sf_body = st.text_area("body", label_visibility="collapsed", key="_sf_body", height=300)

    with st.expander("Live .md preview"):
        preview_meta = {
            "name": sf_name or "skill_name",
            "display_name": sf_display or "Skill Name",
            "version": sf_version or "1.0.0",
            "description": sf_desc,
            "domain": sf_domain,
            "test_prompts": [p.strip() for p in sf_prompts_raw.splitlines() if p.strip()],
        }
        preview_yaml = __import__("yaml").dump(preview_meta, default_flow_style=False, allow_unicode=True, sort_keys=False)
        indented_preview_sql = "\n".join("  " + line for line in (sf_sql or "").splitlines())
        st.code(
            f"---\n{preview_yaml}sql_template: |\n{indented_preview_sql}\n---\n\n{sf_body or ''}",
            language="yaml",
        )

    # ── Build fields dict and run validation ─────────────────────────────
    fields = {
        "name": sf_name.strip(),
        "display_name": sf_display.strip(),
        "version": sf_version.strip() or "1.0.0",
        "description": sf_desc.strip(),
        "domain": sf_domain,
        "inputs": d.get("inputs", []),
        "test_prompts": [p.strip() for p in sf_prompts_raw.splitlines() if p.strip()],
        "sql_template": sf_sql.strip(),
        "body": sf_body.strip(),
        "created_by": "manual_authoring",
        "created_at": str(date.today()),
    }

    old_skill = skills.get(editing_name) if editing_name else None
    con_for_validation = get_warehouse() if st.session_state.data_mode == "demo" else None
    vr = validate_skill_fields(
        fields,
        con=con_for_validation,
        existing_skills=skills,
        old_skill=old_skill,
    )

    # ── Validation display ────────────────────────────────────────────────
    st.divider()
    st.subheader("Validation")
    if vr.errors:
        for e in vr.errors:
            st.error(f"❌ {e}")
    if vr.warnings:
        for w in vr.warnings:
            st.warning(f"⚠️ {w}")
    if vr.info:
        for i in vr.info:
            st.info(f"ℹ️ {i}")
    if vr.valid and not vr.warnings and not vr.info:
        st.success("✓ All checks passed.")

    # Version bump hint
    if old_skill and sf_sql.strip() != old_skill.sql_template.strip():
        from engine import _bump_patch
        st.caption(f"SQL changed — you must bump the version. Suggested: **{_bump_patch(old_skill.version)}**")

    # ── Save button ───────────────────────────────────────────────────────
    st.divider()
    save_col, cancel_col = st.columns([1, 5])
    save_disabled = not vr.valid
    if save_col.button("Save Skill", type="primary", disabled=save_disabled):
        _save_err = None
        _saved = None
        try:
            _saved = save_skill_file(fields, overwrite_name=editing_name)
            get_skills.clear()
            st.session_state.editing_skill_name = None
            st.session_state.add_skill_draft = {}
            st.session_state.add_skill_draft_id = None
        except Exception as exc:
            _save_err = str(exc)
        if _save_err:
            st.error(f"Could not save skill: {_save_err}")
        elif _saved:
            st.session_state.current_page = "Skill Library"
            st.rerun()

    if cancel_col.button("Cancel"):
        st.session_state.editing_skill_name = None
        st.session_state.add_skill_draft = {}
        st.session_state.add_skill_draft_id = None
        st.session_state.current_page = "Skill Library"
        st.rerun()


# ============ PAGE 4: EVALS ============
elif page == "Evals":
    st.title("Eval Dashboard")
    st.caption("Routing accuracy across all test prompts. Run on every commit — no silent regressions.")

    evals = get_eval_results()
    if not evals:
        st.warning("No eval results yet. Run `python run_evals.py` to generate them.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tests", evals["total_tests"])
        c2.metric("Passed", evals["passed"], delta=f"{evals['accuracy']*100:.1f}%")
        c3.metric("Avg latency", f"{evals['avg_latency_ms']} ms")
        c4.metric("Avg confidence", f"{evals['avg_confidence']*100:.1f}%")
        st.caption(f"Last run: {evals['run_at']}")
        st.divider()

        st.subheader("Accuracy by skill")
        per_skill = evals["per_skill"]
        skill_df = pd.DataFrame([
            {
                "Skill": v["display_name"],
                "Accuracy %": v["accuracy"] * 100,
                "Passed": v["passed"],
                "Total": v["total"],
                "Avg Latency (ms)": v["avg_latency_ms"],
            }
            for v in per_skill.values()
        ])
        fig = px.bar(
            skill_df, x="Skill", y="Accuracy %",
            color_discrete_sequence=["#b8ff3c"], template="plotly_dark", text="Accuracy %",
        )
        fig.update_traces(texttemplate="%{text:.0f}%", textposition="outside")
        fig.update_layout(
            plot_bgcolor="#11140f", paper_bgcolor="#11140f",
            font_color="#e8e9e3", yaxis_range=[0, 110], height=380,
            margin=dict(t=20, b=20, l=20, r=20),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(skill_df, use_container_width=True, hide_index=True)
        st.divider()

        st.subheader("Per-prompt results")
        results_df = pd.DataFrame(evals["results"])
        results_df["status"] = results_df["passed"].apply(lambda x: "✓ PASS" if x else "✗ FAIL")
        results_df = results_df[[
            "status", "expected_skill", "prompt", "predicted_skill",
            "confidence", "latency_ms", "reasoning",
        ]]
        only_failures = st.checkbox("Show only failures")
        if only_failures:
            results_df = results_df[results_df["status"] == "✗ FAIL"]
        st.dataframe(
            results_df, use_container_width=True, hide_index=True,
            column_config={
                "confidence": st.column_config.ProgressColumn(
                    "Confidence", min_value=0, max_value=1, format="%.2f"
                ),
                "latency_ms": st.column_config.NumberColumn("Latency (ms)"),
            },
        )


# ============ PAGE 4: WAREHOUSE INSPECTOR ============
elif page == "Warehouse":
    st.title("Connected Warehouse")
    st.caption("Synthetic SaaS finance warehouse running in DuckDB.")

    con = get_warehouse()
    stats = get_warehouse_stats(con)

    cols = st.columns(4)
    cols[0].metric("Customers", f"{int(stats['customers']):,}")
    cols[1].metric("Active customers", f"{int(stats['active_customers']):,}")
    cols[2].metric("Subscriptions", f"{int(stats['subscriptions']):,}")
    cols[3].metric("Current ARR", f"${float(stats['current_arr'])/1e6:.1f}M")

    cols2 = st.columns(4)
    cols2[0].metric("Active subs", f"{int(stats['active_subs']):,}")
    cols2[1].metric("Invoices", f"{int(stats['invoices']):,}")
    cols2[2].metric("HC snapshots", f"{int(stats['headcount_snapshots']):,}")

    st.divider()
    st.subheader("Tables")
    tables = ["customers", "subscriptions", "invoices", "headcount"]
    tabs = st.tabs(tables)
    for tab, table in zip(tabs, tables):
        with tab:
            df = con.execute(f"SELECT * FROM {table} LIMIT 20").fetchdf()
            st.caption(f"Showing 20 rows from `{table}`")
            st.dataframe(df, use_container_width=True, hide_index=True)


# ============ PAGE 5: CONNECT DATA ============
elif page == "Connect Data":
    st.title("Connect Data")
    st.caption("Switch between the built-in demo warehouse and your own CSV files.")

    # ---- Mode toggle ----
    mode_choice = st.radio(
        "Data source",
        ["Demo Warehouse", "My Data (upload CSVs)"],
        index=0 if st.session_state.data_mode == "demo" else 1,
        horizontal=True,
    )
    new_mode = "demo" if mode_choice == "Demo Warehouse" else "my_data"
    if new_mode != st.session_state.data_mode:
        st.session_state.data_mode = new_mode
        st.rerun()

    st.divider()

    # ---- Demo mode info ----
    if st.session_state.data_mode == "demo":
        st.markdown('<span class="mode-demo">● DEMO MODE ACTIVE</span>', unsafe_allow_html=True)
        st.markdown("""
The demo warehouse is a synthetic SaaS finance dataset with 400 customers, ~800 subscriptions, invoices, and monthly headcount snapshots.

All 5 skills (Churn, NRR, ARR by Segment, Headcount Variance, Top Customers) work against this data out of the box.
        """)
        con = get_warehouse()
        stats = get_warehouse_stats(con)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Customers", f"{int(stats['customers']):,}")
        c2.metric("Active customers", f"{int(stats['active_customers']):,}")
        c3.metric("Subscriptions", f"{int(stats['subscriptions']):,}")
        c4.metric("Current ARR", f"${float(stats['current_arr'])/1e6:.1f}M")

        st.divider()
        st.subheader("Download sample CSVs")
        st.caption("Download any of the demo tables, re-upload them in My Data mode, and test the full flow end-to-end.")

        dl_cols = st.columns(4)
        for col_widget, table in zip(dl_cols, ["customers", "subscriptions", "invoices", "headcount"]):
            df_dl = con.execute(f"SELECT * FROM {table}").fetchdf()
            col_widget.download_button(
                label=f"{table}.csv",
                data=df_dl.to_csv(index=False).encode("utf-8"),
                file_name=f"{table}.csv",
                mime="text/csv",
                use_container_width=True,
                key=f"dl_{table}",
            )

    # ---- My Data mode ----
    else:
        st.markdown('<span class="mode-mydata">● MY DATA MODE ACTIVE</span>', unsafe_allow_html=True)
        st.markdown("Upload one or more CSV files. Each file becomes a table named after the filename.")

        _MAX_FILE_BYTES = 50 * 1024 * 1024  # 50 MB

        uploaded_files = st.file_uploader(
            "Drop CSVs here",
            type=["csv"],
            accept_multiple_files=True,
            label_visibility="collapsed",
            help="Max 50 MB per file",
        )

        if uploaded_files:
            new_dfs = {}
            new_date_cols = {}

            for f in uploaded_files:
                if f.size > _MAX_FILE_BYTES:
                    st.error(
                        f"**{f.name}** is {f.size / 1024 / 1024:.1f} MB — exceeds the 50 MB limit. "
                        "Trim the file or split it into smaller tables before uploading."
                    )
                    continue

                raw_name = Path(f.name).stem
                # Sanitize table name: replace non-alphanumeric with _
                tname = re.sub(r"[^a-zA-Z0-9]", "_", raw_name).lower().strip("_")

                try:
                    df = pd.read_csv(io.StringIO(f.read().decode("utf-8", errors="replace")))
                except Exception as e:
                    st.error(f"Could not parse `{f.name}`: {e}")
                    continue

                df, cast_cols = _auto_cast_dates(df)
                new_dfs[tname] = df
                new_date_cols[tname] = cast_cols

            if new_dfs:
                st.session_state.user_dfs = new_dfs
                st.session_state.user_date_cols = new_date_cols
                st.session_state.suggested_qs = []
                st.session_state.data_profile = None   # reset; re-run profiler below
                st.session_state.rel_decisions = {}
                client_upload = get_anthropic_client()
                if client_upload:
                    schema_str = build_user_schema(new_dfs, new_date_cols)
                    with st.spinner("Generating suggested questions…"):
                        st.session_state.suggested_qs = generate_suggested_questions(client_upload, schema_str)

        # ---- Clear button ----
        if st.session_state.user_dfs:
            if st.button("Clear uploaded data and return to Demo mode", type="secondary"):
                st.session_state.user_dfs = {}
                st.session_state.user_date_cols = {}
                st.session_state.suggested_qs = []
                st.session_state.data_profile = None
                st.session_state.rel_decisions = {}
                st.session_state.data_mode = "demo"
                st.rerun()

        # ---- Data Profile ----
        if st.session_state.user_dfs:
            # Run profiler once per upload (result cached in session state)
            if st.session_state.data_profile is None:
                with st.spinner("Profiling your data…"):
                    _skills_for_profile = get_skills()
                    _user_con_for_profile = get_user_con()
                    st.session_state.data_profile = profile_uploaded_data(
                        st.session_state.user_dfs,
                        _skills_for_profile,
                        _user_con_for_profile,
                    )

            profile = st.session_state.data_profile
            st.divider()
            st.subheader("Data Profile")

            # ── Tables ───────────────────────────────────────────────────
            st.markdown("**Tables**")
            tbl_rows = []
            for tname, tmeta in profile["tables"].items():
                date_cols = st.session_state.user_date_cols.get(tname, [])
                tbl_rows.append({
                    "Table": tname,
                    "Rows": f"{tmeta['rows']:,}",
                    "Columns": tmeta["cols"],
                    "PK Candidate(s)": ", ".join(tmeta["pk_candidates"]) or "—",
                    "Date columns (auto-cast)": ", ".join(date_cols) or "—",
                })
            st.dataframe(pd.DataFrame(tbl_rows), use_container_width=True, hide_index=True)

            # ── Relationships ─────────────────────────────────────────────
            st.markdown("**Detected Relationships**")
            rels = profile["relationships"]
            if not rels:
                st.caption("No FK relationships detected.")
            else:
                st.caption(
                    "Relationships are inferred by column-name similarity and value overlap. "
                    "Accept the ones that are correct — accepted relationships are included in the schema "
                    "description sent to the SQL generator."
                )
                for rel in rels:
                    rel_id = f"{rel['from_table']}.{rel['from_col']}__{rel['to_table']}.{rel['to_col']}"
                    decision = st.session_state.rel_decisions.get(rel_id, "pending")
                    conf_pct = int(rel["confidence"] * 100)
                    badge = "✅" if decision == "accepted" else ("❌" if decision == "rejected" else "⬜")
                    with st.container(border=True):
                        rc1, rc2, rc3, rc4, rc5, rc6 = st.columns([4, 2, 2, 2, 1, 1])
                        rc1.markdown(
                            f"{badge} `{rel['from_table']}.{rel['from_col']}` → "
                            f"`{rel['to_table']}.{rel['to_col']}`"
                        )
                        rc2.metric("Confidence", f"{conf_pct}%")
                        rc3.metric("Name match", f"{int(rel['name_score'])}%")
                        rc4.metric("Value overlap", f"{rel['overlap_pct']}%")
                        if rc5.button("✓", key=f"acc_{rel_id}", help="Accept"):
                            st.session_state.rel_decisions[rel_id] = "accepted"
                            st.rerun()
                        if rc6.button("✗", key=f"rej_{rel_id}", help="Reject"):
                            st.session_state.rel_decisions[rel_id] = "rejected"
                            st.rerun()

            # ── Skill compatibility matrix ────────────────────────────────
            st.markdown("**Skill Compatibility**")
            compat = profile["skill_compat"]
            if not compat:
                st.caption("No skills loaded.")
            else:
                compat_rows = []
                for sk_name, sc in compat.items():
                    if sc["status"] == "compatible":
                        status_icon = "✅ Compatible"
                    elif sc["status"] == "partial":
                        status_icon = "⚠️ Partial"
                    else:
                        status_icon = "❌ Incompatible"
                    compat_rows.append({
                        "Skill": sc["display_name"],
                        "Status": status_icon,
                        "Issues": "; ".join(sc["issues"]) if sc["issues"] else "—",
                    })
                st.dataframe(pd.DataFrame(compat_rows), use_container_width=True, hide_index=True)
                n_compat = sum(1 for sc in compat.values() if sc["status"] == "compatible")
                if n_compat == 0:
                    st.warning(
                        "No skills are compatible with your schema. "
                        "The AI will generate custom SQL instead."
                    )
                elif n_compat < len(compat):
                    st.info(f"{n_compat}/{len(compat)} skills compatible — incompatible skills will fall back to AI-generated SQL.")
                else:
                    st.success(f"All {n_compat} skills are compatible with your schema.")

            # ── Suggested questions ───────────────────────────────────────
            if st.session_state.suggested_qs:
                st.markdown("**Suggested questions**")
                st.caption("Claude inspected your schema and suggested these — click to pre-fill the Ask page.")
                for i, q in enumerate(st.session_state.suggested_qs):
                    if st.button(q, key=f"sugg_{i}", use_container_width=True):
                        st.session_state.user_question = q
                        st.session_state.data_mode = "my_data"

            # ── Confirm ───────────────────────────────────────────────────
            st.divider()
            if st.button("Confirm & Use This Data →", type="primary"):
                st.session_state.current_page = "Ask"
                st.rerun()

        else:
            st.info("No files uploaded yet. Drag and drop CSVs above.")
