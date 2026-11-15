"""
streamlit_app.py
----------------
Streamlit version of the Sales ETL GUI, built for deployment
(Streamlit Community Cloud, Docker, any server).

  - Upload a raw CSV (or use the bundled sample) -- no local file dialogs
  - Run the ETL pipeline with a live log
  - Browse star-schema tables and run read-only SQL
  - View charts: revenue trend, top products, regional sales

Run locally:
    streamlit run src/streamlit_app.py

Deploy (Streamlit Community Cloud):
    Main file path: src/streamlit_app.py
"""

import sqlite3
import tempfile
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from etl_pipeline import run_pipeline

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_CSV = PROJECT_ROOT / "data" / "raw" / "sales_data.csv"

ACCENT = "#7aa2f7"
PANEL = "#282a3a"
TEXT = "#e8e8ec"
MUTED = "#9aa0b4"

st.set_page_config(page_title="Sales Data ETL Pipeline", page_icon="📊", layout="wide")


# ----------------------------------------------------------------------
# Session state
# ----------------------------------------------------------------------
def _init_state():
    if "workdir" not in st.session_state:
        # Per-session scratch space: deployed filesystems are ephemeral
        # and shared between visitors, so never write into the repo.
        st.session_state.workdir = Path(tempfile.mkdtemp(prefix="sales_etl_"))
    st.session_state.setdefault("db_path", None)
    st.session_state.setdefault("log", [])
    st.session_state.setdefault("db_version", 0)  # busts the query cache after each run
    st.session_state.setdefault("sql_text", "SELECT * FROM fact_sales LIMIT 20;")


_init_state()


# ----------------------------------------------------------------------
# DB helpers
# ----------------------------------------------------------------------
def _connect_readonly(db_path):
    """Open the DB read-only so user-typed SQL can't modify or drop anything."""
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


@st.cache_data(show_spinner=False)
def query_df(db_path: str, sql: str, version: int) -> pd.DataFrame:
    # `version` is only part of the cache key
    conn = _connect_readonly(db_path)
    try:
        return pd.read_sql_query(sql, conn)
    finally:
        conn.close()


def list_tables(db_path):
    conn = _connect_readonly(db_path)
    try:
        return [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")]
    finally:
        conn.close()


def db_ready():
    p = st.session_state.db_path
    return p is not None and Path(p).exists()


# ----------------------------------------------------------------------
# ETL runner
# ----------------------------------------------------------------------
def run_etl(csv_path: Path):
    db_path = st.session_state.workdir / "sales.db"
    st.session_state.log = ["--- Starting pipeline ---"]
    log_box = st.empty()

    def log(msg):
        st.session_state.log.append(str(msg))
        log_box.code("\n".join(st.session_state.log), language="text")

    log_box.code("\n".join(st.session_state.log), language="text")
    try:
        if db_path.exists():
            db_path.unlink()  # fresh DB each run
        run_pipeline(csv_path, db_path, log_callback=log)
        log("--- Pipeline finished successfully ---")
        st.session_state.db_path = str(db_path)
        st.session_state.db_version += 1
        query_df.clear()
        return True
    except Exception as e:  # noqa: BLE001
        log(f"ERROR: {e}")
        st.error(f"Pipeline failed: {e}")
        return False


# ----------------------------------------------------------------------
# Charts
# ----------------------------------------------------------------------
def _style_ax(fig, ax, title):
    fig.patch.set_facecolor(PANEL)
    ax.set_facecolor(PANEL)
    ax.set_title(title, color=TEXT)
    ax.tick_params(colors=TEXT)
    for spine in ax.spines.values():
        spine.set_color(MUTED)


def chart_revenue_trend(df):
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.plot(df["period"], df["revenue"], color=ACCENT, marker="o", linewidth=2)
    _style_ax(fig, ax, "Monthly Revenue Trend")
    ax.tick_params(axis="x", rotation=60, labelsize=7)
    fig.tight_layout()
    return fig


def chart_top_products(df):
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.barh(df["product_name"][::-1], df["revenue"][::-1], color=ACCENT)
    _style_ax(fig, ax, "Top 10 Products by Revenue")
    ax.tick_params(labelsize=8)
    fig.tight_layout()
    return fig


def chart_regional_sales(df):
    fig, ax = plt.subplots(figsize=(6, 5))
    colors = plt.cm.Blues([max(0.2, 0.9 - i * 0.13) for i in range(len(df))])
    ax.pie(df["revenue"], labels=df["region_name"], autopct="%1.1f%%",
           colors=colors, textprops={"color": TEXT})
    fig.patch.set_facecolor(PANEL)
    ax.set_title("Revenue Share by Region", color=TEXT)
    fig.tight_layout()
    return fig


CHARTS = {
    "Revenue Trend": (
        """
        SELECT d.year || '-' || printf('%02d', d.month) AS period,
               ROUND(SUM(f.revenue),2) AS revenue
        FROM fact_sales f JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.year, d.month ORDER BY d.year, d.month;
        """,
        chart_revenue_trend,
    ),
    "Top Products": (
        """
        SELECT p.product_name, ROUND(SUM(f.revenue),2) AS revenue
        FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_id ORDER BY revenue DESC LIMIT 10;
        """,
        chart_top_products,
    ),
    "Regional Sales": (
        """
        SELECT r.region_name, ROUND(SUM(f.revenue),2) AS revenue
        FROM fact_sales f JOIN dim_region r ON f.region_id = r.region_id
        GROUP BY r.region_id ORDER BY revenue DESC;
        """,
        chart_regional_sales,
    ),
}


# ----------------------------------------------------------------------
# UI
# ----------------------------------------------------------------------
st.title("📊 Sales Data ETL Pipeline")
st.caption("CSV → Pandas (clean/transform) → SQLite star schema → SQL reporting")

# --- Sidebar: input + run ---
with st.sidebar:
    st.header("1. Choose data")
    options = ["Upload CSV"]
    if SAMPLE_CSV.exists():
        options.append("Use sample data")
    source = st.radio("Source", options, label_visibility="collapsed")

    uploaded = None
    if source == "Upload CSV":
        uploaded = st.file_uploader("Raw sales CSV", type=["csv"])

    st.header("2. Run")
    run_clicked = st.button("▶ Run ETL Pipeline", type="primary", use_container_width=True)

    if db_ready():
        st.success("Database ready")
        with open(st.session_state.db_path, "rb") as f:
            st.download_button("⬇ Download SQLite DB", f.read(),
                               file_name="sales.db", mime="application/x-sqlite3",
                               use_container_width=True)

# --- Resolve CSV and run ---
csv_to_run = None
if run_clicked:
    if source == "Use sample data":
        csv_to_run = SAMPLE_CSV
    elif uploaded is not None:
        csv_to_run = st.session_state.workdir / "uploaded.csv"
        csv_to_run.write_bytes(uploaded.getvalue())
    else:
        st.warning("Please upload a CSV file first.")

tab_log, tab_sql, tab_charts = st.tabs(["Pipeline Log", "SQL Explorer", "Charts (Power BI-style)"])

# --- Log tab ---
with tab_log:
    if csv_to_run is not None:
        with st.spinner("Running pipeline…"):
            run_etl(csv_to_run)
    elif st.session_state.log:
        st.code("\n".join(st.session_state.log), language="text")
    else:
        st.info("Ready. Choose a CSV in the sidebar and click **Run ETL Pipeline**.")

# --- SQL tab ---
with tab_sql:
    if not db_ready():
        st.info("Run the ETL pipeline first.")
    else:
        db = st.session_state.db_path
        tables = list_tables(db)

        def _on_table_change():
            st.session_state.sql_text = f"SELECT * FROM {st.session_state.table_choice} LIMIT 50;"

        st.selectbox("Table", tables, key="table_choice", on_change=_on_table_change)
        st.text_area("SQL query (read-only)", key="sql_text", height=140)

        if st.button("▶ Run SQL"):
            try:
                df = query_df(db, st.session_state.sql_text.strip(),
                              st.session_state.db_version)
                st.caption(f"{len(df):,} rows × {len(df.columns)} columns")
                st.dataframe(df, use_container_width=True)
            except Exception as e:  # noqa: BLE001
                st.error(f"SQL error: {e}")

# --- Charts tab ---
with tab_charts:
    if not db_ready():
        st.info("Run the ETL pipeline first.")
    else:
        choice = st.radio("Chart", list(CHARTS), horizontal=True)
        sql, builder = CHARTS[choice]
        try:
            df = query_df(st.session_state.db_path, sql, st.session_state.db_version)
            if df.empty:
                st.warning("No data to chart.")
            else:
                fig = builder(df)
                st.pyplot(fig)
                plt.close(fig)
        except Exception as e:  # noqa: BLE001
            st.error(f"Could not build chart: {e}")
