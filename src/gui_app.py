"""
app.py
------
Streamlit web app for the Sales Data ETL Pipeline.

Sections:
  1. Run Pipeline   - upload Source 1 and Source 2 CSVs separately, run extract/transform/load
  2. SQL Explorer   - browse tables or run any SQL query against the star schema
  3. Graph Analysis - trend, category, region, correlation, distribution charts

Run locally:  streamlit run src/app.py
"""

import io
import sqlite3
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st

from etl_pipeline import run_pipeline

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_DB = PROJECT_ROOT / "data" / "sales.db"

REQUIRED_COLUMNS = {"order_id", "order_date", "product_name", "category",
                    "region", "customer_name", "quantity", "unit_price"}

st.set_page_config(page_title="Sales ETL Pipeline", page_icon="📊", layout="wide")
sns.set_theme(style="darkgrid")

# --------------------------------------------------------------------------
# Sidebar - pipeline controls
# --------------------------------------------------------------------------
st.sidebar.title("📊 Sales ETL Pipeline")
st.sidebar.caption("CSV sources → Pandas clean/transform → SQLite star schema → SQL + graph analysis")

db_path = Path(st.sidebar.text_input("SQLite DB path", value=str(DEFAULT_DB)))

st.sidebar.subheader("1. Raw CSV sources")

src1 = st.sidebar.file_uploader("Source 1 CSV (e.g. online sales)", type="csv", key="src1")
src2 = st.sidebar.file_uploader("Source 2 CSV (e.g. retail store sales)", type="csv", key="src2")


def check_upload(uf, label):
    """Validate columns; return the set of order_ids (empty set if invalid)."""
    try:
        df = pd.read_csv(io.BytesIO(uf.getvalue()))
    except Exception as e:
        st.sidebar.error(f"{label}: could not read CSV ({e})")
        return None
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        st.sidebar.error(f"{label} is missing columns: {', '.join(sorted(missing))}")
        return None
    st.sidebar.success(f"{label}: {len(df):,} rows ✓")
    return set(df["order_id"].dropna())


ids1 = check_upload(src1, "Source 1") if src1 else None
ids2 = check_upload(src2, "Source 2") if src2 else None

if ids1 and ids2:
    overlap = ids1 & ids2
    if overlap:
        st.sidebar.warning(
            f"{len(overlap):,} order_ids appear in BOTH files. The pipeline drops "
            "duplicate order_ids, so those rows would be lost. Use unique IDs per source."
        )

uploads_present = src1 is not None or src2 is not None
existing_csvs = sorted(str(p) for p in RAW_DIR.glob("*.csv")) if RAW_DIR.exists() else []
selected_existing = st.sidebar.multiselect(
    "…or use files already in data/raw/",
    options=existing_csvs,
    default=[] if uploads_present else existing_csvs,
)

run_clicked = st.sidebar.button("▶ Run ETL Pipeline", type="primary")

if "log_lines" not in st.session_state:
    st.session_state.log_lines = []

if run_clicked:
    csv_paths = list(selected_existing)
    used_names = {Path(p).name for p in csv_paths}

    # Save each uploaded file under its own name so source_system stays distinct
    for uf, slot, ids in ((src1, "source1", ids1), (src2, "source2", ids2)):
        if uf is None or ids is None:
            continue
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        name = uf.name
        if name in used_names:
            p = Path(name)
            name = f"{p.stem}_{slot}{p.suffix}"
        used_names.add(name)
        dest = RAW_DIR / name
        dest.write_bytes(uf.getvalue())
        csv_paths.append(str(dest))

    if not csv_paths:
        st.sidebar.error("Upload at least one valid CSV (Source 1 and/or Source 2).")
    else:
        st.session_state.log_lines = []

        def log_callback(msg):
            st.session_state.log_lines.append(msg)

        with st.spinner("Running extract → transform → load…"):
            try:
                run_pipeline([Path(p) for p in csv_paths], db_path, log_callback=log_callback)
                st.sidebar.success(f"Pipeline finished using {len(csv_paths)} source file(s).")
            except Exception as e:
                st.sidebar.error(f"Pipeline failed: {e}")


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def db_ready():
    return db_path.exists()


def query(sql: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        return pd.read_sql_query(sql, conn)
    finally:
        conn.close()


def list_tables():
    return query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")["name"].tolist()


# --------------------------------------------------------------------------
# Main layout
# --------------------------------------------------------------------------
tab_log, tab_sql, tab_charts = st.tabs(["🪵 Pipeline Log", "🔎 SQL Explorer", "📈 Graph Analysis"])

with tab_log:
    st.subheader("Pipeline run log")
    if st.session_state.log_lines:
        st.code("\n".join(st.session_state.log_lines), language="text")
    else:
        st.info("No pipeline run yet. Upload Source 1 and Source 2 in the sidebar, then click **Run ETL Pipeline**.")

    if db_ready():
        st.subheader("Current tables in the database")
        for t in list_tables():
            n = query(f"SELECT COUNT(*) AS n FROM {t}")["n"][0]
            st.write(f"- **{t}** — {n:,} rows")

with tab_sql:
    if not db_ready():
        st.warning("Run the ETL pipeline first — no database found yet.")
    else:
        tables = list_tables()
        col1, col2 = st.columns([1, 3])
        with col1:
            chosen_table = st.selectbox("Table", tables)
            if st.button("Preview table"):
                st.session_state["sql_text"] = f"SELECT * FROM {chosen_table} LIMIT 50;"

        default_sql = st.session_state.get("sql_text", "SELECT * FROM fact_sales LIMIT 20;")
        sql_text = st.text_area("SQL query", value=default_sql, height=120)

        if st.button("▶ Run SQL", type="primary"):
            try:
                result = query(sql_text)
                st.dataframe(result)
                st.caption(f"{len(result):,} rows returned")
                st.download_button("Download results as CSV",
                                   result.to_csv(index=False).encode(),
                                   "query_results.csv", "text/csv")
            except Exception as e:
                st.error(f"SQL error: {e}")

        sql_file = PROJECT_ROOT / "sql" / "queries.sql"
        with st.expander("Sample reporting queries (sql/queries.sql)"):
            st.code(sql_file.read_text() if sql_file.exists() else "sql/queries.sql not found",
                    language="sql")

with tab_charts:
    if not db_ready():
        st.warning("Run the ETL pipeline first — no database found yet.")
    else:
        analysis = st.selectbox("Choose an analysis", [
            "Revenue Trend (monthly)",
            "Top Products by Revenue",
            "Revenue by Region",
            "Revenue by Category & Quarter (heatmap)",
            "Region x Category Revenue (heatmap)",
            "Unit Price vs Quantity (correlation / scatter)",
            "Order Value Distribution",
            "Top Customers by Spend",
            "Revenue by Source System",
        ])

        fig, ax = plt.subplots(figsize=(10, 5.5))

        if analysis == "Revenue Trend (monthly)":
            df = query("""
                SELECT d.year || '-' || printf('%02d', d.month) AS period,
                       ROUND(SUM(f.revenue),2) AS revenue
                FROM fact_sales f JOIN dim_date d ON f.date_id = d.date_id
                GROUP BY d.year, d.month ORDER BY d.year, d.month;""")
            ax.plot(df["period"], df["revenue"], marker="o")
            ax.set_title("Monthly Revenue Trend")
            ax.set_ylabel("Revenue")
            plt.xticks(rotation=60, fontsize=8)

        elif analysis == "Top Products by Revenue":
            df = query("""
                SELECT p.product_name, ROUND(SUM(f.revenue),2) AS revenue
                FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
                GROUP BY p.product_id ORDER BY revenue DESC LIMIT 10;""")
            sns.barplot(data=df, y="product_name", x="revenue", ax=ax, orient="h")
            ax.set_title("Top 10 Products by Revenue")

        elif analysis == "Revenue by Region":
            df = query("""
                SELECT r.region_name, ROUND(SUM(f.revenue),2) AS revenue
                FROM fact_sales f JOIN dim_region r ON f.region_id = r.region_id
                GROUP BY r.region_id ORDER BY revenue DESC;""")
            ax.pie(df["revenue"], labels=df["region_name"], autopct="%1.1f%%")
            ax.set_title("Revenue Share by Region")

        elif analysis == "Revenue by Category & Quarter (heatmap)":
            df = query("""
                SELECT d.year || ' Q' || d.quarter AS period, p.category,
                       ROUND(SUM(f.revenue),2) AS revenue
                FROM fact_sales f
                JOIN dim_date d ON f.date_id = d.date_id
                JOIN dim_product p ON f.product_id = p.product_id
                GROUP BY period, p.category;""")
            pivot = df.pivot(index="category", columns="period", values="revenue").fillna(0)
            sns.heatmap(pivot, annot=True, fmt=".0f", cmap="mako", ax=ax)
            ax.set_title("Revenue by Category & Quarter")

        elif analysis == "Region x Category Revenue (heatmap)":
            df = query("""
                SELECT r.region_name, p.category, ROUND(SUM(f.revenue),2) AS revenue
                FROM fact_sales f
                JOIN dim_region r ON f.region_id = r.region_id
                JOIN dim_product p ON f.product_id = p.product_id
                GROUP BY r.region_name, p.category;""")
            pivot = df.pivot(index="region_name", columns="category", values="revenue").fillna(0)
            sns.heatmap(pivot, annot=True, fmt=".0f", cmap="rocket_r", ax=ax)
            ax.set_title("Revenue: Region x Category")

        elif analysis == "Unit Price vs Quantity (correlation / scatter)":
            df = query("SELECT unit_price, quantity, revenue FROM fact_sales;")
            sns.scatterplot(data=df, x="unit_price", y="quantity", hue="revenue",
                            palette="viridis", alpha=0.6, ax=ax)
            ax.set_title(f"Unit Price vs Quantity (r = {df['unit_price'].corr(df['quantity']):.3f})")

        elif analysis == "Order Value Distribution":
            df = query("SELECT revenue FROM fact_sales;")
            sns.histplot(df["revenue"], bins=40, kde=True, ax=ax)
            ax.set_title("Distribution of Order Line Revenue")

        elif analysis == "Top Customers by Spend":
            df = query("""
                SELECT c.customer_name, ROUND(SUM(f.revenue),2) AS total_spend
                FROM fact_sales f JOIN dim_customer c ON f.customer_id = c.customer_id
                GROUP BY c.customer_id ORDER BY total_spend DESC LIMIT 10;""")
            sns.barplot(data=df, y="customer_name", x="total_spend", ax=ax, orient="h")
            ax.set_title("Top 10 Customers by Spend")

        elif analysis == "Revenue by Source System":
            df = query("""
                SELECT source_system, ROUND(SUM(revenue),2) AS revenue
                FROM fact_sales GROUP BY source_system;""")
            sns.barplot(data=df, x="source_system", y="revenue", ax=ax)
            ax.set_title("Revenue by Source System")

        fig.tight_layout()
        st.pyplot(fig)

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150)
        st.download_button("Download chart as PNG", buf.getvalue(), "chart.png", "image/png")
