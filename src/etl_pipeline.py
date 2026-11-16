"""
etl_pipeline.py
----------------
End-to-end ETL pipeline:

    EXTRACT   -> read one or more raw CSV sources and merge them
    TRANSFORM -> clean with Pandas, build a star schema (fact + dimensions)
    LOAD      -> write the schema into a SQLite database

Supports multiple CSV sources (e.g. online + retail exports) in one run.

Run directly:
    python src/etl_pipeline.py \
        --csv data/raw/online_sales.csv data/raw/retail_store_sales.csv \
        --db data/sales.db
"""

import argparse
import logging
import sqlite3
from pathlib import Path
from typing import List, Union

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("etl")


# --------------------------------------------------------------------------
# EXTRACT
# --------------------------------------------------------------------------
def extract(csv_paths: Union[Path, str, List[Union[Path, str]]]) -> pd.DataFrame:
    """Extract and merge one or more raw CSV sources into a single DataFrame.
    Each row is tagged with its originating file in `source_system`."""
    if isinstance(csv_paths, (str, Path)):
        csv_paths = [csv_paths]

    frames = []
    for path in csv_paths:
        path = Path(path)
        logger.info(f"Extracting raw data from {path.name}")
        df = pd.read_csv(path)
        df["source_system"] = path.stem
        logger.info(f"  -> {len(df)} rows from {path.name}")
        frames.append(df)

    merged = pd.concat(frames, ignore_index=True)
    logger.info(f"Extracted {len(merged)} raw rows total from {len(csv_paths)} source(s)")
    return merged


# --------------------------------------------------------------------------
# TRANSFORM
# --------------------------------------------------------------------------
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Handle missing values, duplicates, and type coercion."""
    before = len(df)

    df = df.drop_duplicates(subset=["order_id"]).copy()

    df = df.dropna(subset=["product_name", "region"])
    df = df[(df["product_name"].astype(str).str.strip() != "") &
            (df["region"].astype(str).str.strip() != "")]

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["quantity"] = df["quantity"].fillna(df["quantity"].median()).astype(int)

    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df = df.dropna(subset=["unit_price"])

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df = df.dropna(subset=["order_date"])

    df["revenue"] = (df["quantity"] * df["unit_price"]).round(2)

    after = len(df)
    logger.info(f"Cleaned data: {before} -> {after} rows "
                f"({before - after} removed as duplicates/invalid)")
    return df.reset_index(drop=True)


def build_star_schema(df: pd.DataFrame):
    """Split the flat dataframe into dim_date, dim_product, dim_region,
    dim_customer and fact_sales."""
    logger.info("Building star schema (dimension + fact tables)")

    # --- dim_date ---
    dates = df["order_date"].dt.normalize().drop_duplicates().sort_values().reset_index(drop=True)
    dim_date = pd.DataFrame({"order_date": dates})
    dim_date["date_id"] = dim_date.index + 1
    dim_date["year"] = dim_date["order_date"].dt.year
    dim_date["month"] = dim_date["order_date"].dt.month
    dim_date["day"] = dim_date["order_date"].dt.day
    dim_date["quarter"] = dim_date["order_date"].dt.quarter
    dim_date["month_name"] = dim_date["order_date"].dt.strftime("%B")
    dim_date["order_date"] = dim_date["order_date"].dt.strftime("%Y-%m-%d")
    dim_date = dim_date[["date_id", "order_date", "year", "month", "month_name", "quarter", "day"]]

    # --- dim_product ---
    dim_product = df[["product_name", "category"]].drop_duplicates().reset_index(drop=True)
    dim_product["product_id"] = dim_product.index + 1
    dim_product = dim_product[["product_id", "product_name", "category"]]

    # --- dim_region ---
    dim_region = pd.DataFrame({"region_name": df["region"].drop_duplicates().reset_index(drop=True)})
    dim_region["region_id"] = dim_region.index + 1
    dim_region = dim_region[["region_id", "region_name"]]

    # --- dim_customer ---
    dim_customer = pd.DataFrame({"customer_name": df["customer_name"].drop_duplicates().reset_index(drop=True)})
    dim_customer["customer_id"] = dim_customer.index + 1
    dim_customer = dim_customer[["customer_id", "customer_name"]]

    # --- fact_sales ---
    fact = df.copy()
    fact["order_date_norm"] = fact["order_date"].dt.strftime("%Y-%m-%d")

    fact = fact.merge(dim_date[["date_id", "order_date"]], left_on="order_date_norm",
                      right_on="order_date", how="left", suffixes=("", "_dim"))
    fact = fact.merge(dim_product, on=["product_name", "category"], how="left")
    fact = fact.merge(dim_region, left_on="region", right_on="region_name", how="left")
    fact = fact.merge(dim_customer, on="customer_name", how="left")

    fact_sales = fact[[
        "order_id", "date_id", "product_id", "region_id", "customer_id",
        "quantity", "unit_price", "revenue", "source_system"
    ]].reset_index(drop=True)
    fact_sales.insert(0, "sale_id", fact_sales.index + 1)

    logger.info(
        f"Star schema built: dim_date={len(dim_date)}, dim_product={len(dim_product)}, "
        f"dim_region={len(dim_region)}, dim_customer={len(dim_customer)}, "
        f"fact_sales={len(fact_sales)}"
    )

    return {
        "dim_date": dim_date,
        "dim_product": dim_product,
        "dim_region": dim_region,
        "dim_customer": dim_customer,
        "fact_sales": fact_sales,
    }


def transform(df: pd.DataFrame):
    return build_star_schema(clean(df))


# --------------------------------------------------------------------------
# LOAD
# --------------------------------------------------------------------------
def load(tables: dict, db_path: Path):
    db_path = Path(db_path)
    logger.info(f"Loading tables into SQLite database at {db_path}")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        for name, df in tables.items():
            df.to_sql(name, conn, if_exists="replace", index=False)
            logger.info(f"  wrote table '{name}' ({len(df)} rows)")

        cur = conn.cursor()
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_sales(date_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_sales(product_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_region ON fact_sales(region_id);")
        conn.commit()
    finally:
        conn.close()
    logger.info("Load complete.")


# --------------------------------------------------------------------------
# PIPELINE ENTRY POINT
# --------------------------------------------------------------------------
def run_pipeline(csv_paths, db_path: Path, log_callback=None):
    """Run extract -> transform -> load.
    csv_paths: a single path or a list of paths (multiple sources are merged).
    log_callback: optional function(str) so a UI can stream progress messages."""
    handler = None
    if log_callback:
        class _UiHandler(logging.Handler):
            def emit(self, record):
                log_callback(self.format(record))
        handler = _UiHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", "%H:%M:%S"))
        logger.addHandler(handler)

    try:
        raw_df = extract(csv_paths)
        tables = transform(raw_df)
        load(tables, Path(db_path))
        return tables
    finally:
        if handler:
            logger.removeHandler(handler)


def main():
    parser = argparse.ArgumentParser(description="Run the sales ETL pipeline.")
    parser.add_argument(
        "--csv", nargs="+",
        default=["data/raw/online_sales.csv", "data/raw/retail_store_sales.csv"],
        help="One or more paths to raw CSV source(s)",
    )
    parser.add_argument("--db", default="data/sales.db", help="Path to output SQLite database")
    args = parser.parse_args()
    run_pipeline([Path(p) for p in args.csv], Path(args.db))


if __name__ == "__main__":
    main()
