# Sales Data ETL Pipeline

An end-to-end ETL pipeline that extracts raw sales data from CSV, cleans and
transforms it with **Pandas**, loads it into a **SQLite** database modeled as
a **star schema**, exposes it through **SQL** reporting queries, and includes
a **Tkinter GUI** to run and explore the whole thing. Key metrics (revenue
trends, top products, regional sales) can be visualized inside the GUI or in
**Power BI**.

This project maps directly onto each resume bullet:

| Resume bullet | Where it lives |
|---|---|
| Built an end-to-end ETL pipeline (CSV → Pandas → SQLite) | `src/etl_pipeline.py` |
| SQL queries for aggregation, joins, reporting; star-schema model | `sql/queries.sql`, `build_star_schema()` in `etl_pipeline.py` |
| Version-controlled with Git, documented for reproducibility | this repo + README |
| Visualized revenue trends, top products, regional sales | GUI "Charts" tab (`src/gui_app.py`) and/or Power BI |

---

## 1. Tools used and what each one does

| Tool | Role in the project |
|---|---|
| **Python** | Glue language for the whole pipeline and GUI |
| **Pandas** | Extract CSV into a DataFrame; clean (dedupe, nulls, types); transform into fact/dimension tables |
| **SQLite (`sqlite3`, built into Python)** | Lightweight relational database that stores the star schema — no server to install |
| **SQL** | Aggregation/reporting queries (`sql/queries.sql`) run directly against SQLite |
| **Tkinter** (standard library) + **Matplotlib** | Desktop GUI: run the pipeline, browse tables, run ad-hoc SQL, view charts |
| **Git** | Version control for the code (see [Git setup](#4-git-setup) below) |
| **Power BI** | Optional: connect to the SQLite file to build interactive dashboards (see [Power BI](#6-connecting-power-bi)) |

---

## 2. Project structure

```
sales_etl_project/
├── data/
│   ├── raw/                  # input CSVs land here (generated or your own)
│   └── sales.db              # output SQLite database (created by the pipeline)
├── src/
│   ├── generate_sample_data.py   # creates a realistic sample CSV (optional)
│   ├── etl_pipeline.py           # extract -> transform -> load
│   └── gui_app.py                # Tkinter GUI wrapping the pipeline
├── sql/
│   └── queries.sql           # aggregation / join / reporting queries
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 3. Setup

```bash
# 1. Clone / enter the project
cd sales_etl_project

# 2. (Recommended) create a virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
```

### Generate sample data (optional)
If you don't have your own sales CSV, generate a realistic one (6,000+ rows,
with intentional messiness — missing fields, duplicates — so the "clean" step
has real work to do):

```bash
python src/generate_sample_data.py
```
This writes `data/raw/sales_data.csv`. Bring your own CSV instead by placing
it anywhere and pointing the pipeline at it — it just needs these columns:
`order_id, order_date, product_name, category, region, customer_name, quantity, unit_price`.

### Run the ETL pipeline (command line)
```bash
python src/etl_pipeline.py --csv data/raw/sales_data.csv --db data/sales.db
```
This will:
1. **Extract** — read the CSV into a Pandas DataFrame
2. **Transform** — drop duplicate/invalid rows, fill missing quantities,
   coerce types, compute `revenue = quantity * unit_price`, then split the
   flat table into a **star schema**: `dim_date`, `dim_product`,
   `dim_region`, `dim_customer`, `fact_sales`
3. **Load** — write all five tables into `data/sales.db`, with indexes on
   the fact table's foreign keys

### Run the GUI (recommended way to use this project)
```bash
python src/gui_app.py
```
The GUI has three tabs:
- **Pipeline Log** — pick your CSV/DB paths and run the pipeline with live logging
- **SQL Explorer** — browse any table or paste a query from `sql/queries.sql` (or your own) and see results in a table
- **Charts** — one-click Revenue Trend, Top Products, and Regional Sales charts, rendered with Matplotlib inside the app

---

## 4. Git setup

```bash
git init
git add .
git commit -m "Initial commit: sales ETL pipeline + GUI"
```
`data/raw/*.csv` and `data/*.db` are excluded via `.gitignore` since they're
generated/reproducible — only the code and sample-data generator are
version-controlled. If you want the sample CSV tracked for reproducibility,
remove that line from `.gitignore`.

---

## 5. The star schema

```
                dim_date            dim_product
              (date_id PK)         (product_id PK)
                    \                    /
                     \                  /
                      \                /
                     fact_sales (sale_id PK)
                      /                \
                     /                  \
              dim_region            dim_customer
            (region_id PK)         (customer_id PK)
```
`fact_sales` holds the measures (`quantity`, `unit_price`, `revenue`) and
foreign keys into each dimension — a standard design for fast aggregation
and joins in BI tools.

---

## 6. Connecting Power BI

Power BI Desktop doesn't have a native SQLite connector, so use one of these:

**Option A — ODBC driver (recommended for a live/refreshable connection)**
1. Install the [SQLite ODBC Driver](http://www.ch-werner.de/sqliteodbc/) (Windows).
2. Create a System DSN pointing at `data/sales.db`.
3. In Power BI: `Get Data → ODBC → select the DSN → pick the tables (fact_sales, dim_date, dim_product, dim_region, dim_customer)`.
4. In Power BI's Model view, draw relationships: `fact_sales[date_id] → dim_date[date_id]`, and similarly for product/region/customer — mirroring the star schema.
5. Build visuals: a line chart of `SUM(revenue)` by `dim_date[month]` for the revenue trend, a bar chart of `SUM(revenue)` by `dim_product[product_name]` (top N filter) for top products, and a map/bar of `SUM(revenue)` by `dim_region[region_name]`.

**Option B — CSV export (quickest, no driver install)**
Export each table for a one-off import:
```bash
python -c "
import sqlite3, pandas as pd
conn = sqlite3.connect('data/sales.db')
for t in ['fact_sales','dim_date','dim_product','dim_region','dim_customer']:
    pd.read_sql_query(f'SELECT * FROM {t}', conn).to_csv(f'data/{t}.csv', index=False)
conn.close()
"
```
Then `Get Data → Text/CSV` in Power BI for each file and set up the same
relationships as in Option A.

---

## 7. Extending this project
- Add an incremental/upsert load instead of full `replace` on each run
- Add data-quality checks (e.g., `great_expectations`) as a validation stage
- Schedule the pipeline (cron / Task Scheduler / Airflow) for recurring runs
- Swap SQLite for Postgres/MySQL by changing the `load()` connection string
- Add a `dim_time` grain finer than day if you need hourly reporting
