"""
gui_app.py
----------
A desktop GUI (Tkinter) that wraps the ETL pipeline so the whole project
can be run and explored without touching the command line:

  - Pick a raw CSV and a SQLite output path
  - Run the ETL pipeline with a live log
  - Browse the resulting star-schema tables
  - Run any SQL query from sql/queries.sql (or type your own) and see results
  - View charts: revenue trend, top products, regional sales
    (the same metrics you'd build out in Power BI)

Run:
    python src/gui_app.py
"""

import sqlite3
import threading
import tkinter as tk
from pathlib import Path
from tkinter import ttk, filedialog, messagebox

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd

from etl_pipeline import run_pipeline

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = PROJECT_ROOT / "data" / "raw" / "sales_data.csv"
DEFAULT_DB = PROJECT_ROOT / "data" / "sales.db"

BG = "#1e1e2e"
PANEL = "#282a3a"
ACCENT = "#7aa2f7"
TEXT = "#e8e8ec"
MUTED = "#9aa0b4"


class SalesETLApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Sales Data ETL Pipeline")
        self.geometry("1150x720")
        self.configure(bg=BG)

        self.csv_path = tk.StringVar(value=str(DEFAULT_CSV))
        self.db_path = tk.StringVar(value=str(DEFAULT_DB))

        self._build_style()
        self._build_layout()

    # ------------------------------------------------------------------
    def _build_style(self):
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("TFrame", background=BG)
        style.configure("Panel.TFrame", background=PANEL)
        style.configure("TLabel", background=BG, foreground=TEXT, font=("Segoe UI", 10))
        style.configure("Panel.TLabel", background=PANEL, foreground=TEXT, font=("Segoe UI", 10))
        style.configure("Header.TLabel", background=BG, foreground=TEXT, font=("Segoe UI", 16, "bold"))
        style.configure("Muted.TLabel", background=BG, foreground=MUTED, font=("Segoe UI", 9))
        style.configure("TButton", font=("Segoe UI", 10), padding=6)
        style.configure("Accent.TButton", background=ACCENT)
        style.configure("TNotebook", background=BG, borderwidth=0)
        style.configure("TNotebook.Tab", padding=(14, 8), font=("Segoe UI", 10))
        style.configure("Treeview", background="#20222e", fieldbackground="#20222e",
                         foreground=TEXT, rowheight=24, font=("Consolas", 9))
        style.configure("Treeview.Heading", background=PANEL, foreground=TEXT, font=("Segoe UI", 9, "bold"))

    # ------------------------------------------------------------------
    def _build_layout(self):
        header = ttk.Frame(self)
        header.pack(fill="x", padx=16, pady=(14, 6))
        ttk.Label(header, text="📊 Sales Data ETL Pipeline", style="Header.TLabel").pack(side="left")
        ttk.Label(header, text="CSV → Pandas (clean/transform) → SQLite star schema → SQL reporting",
                  style="Muted.TLabel").pack(side="left", padx=12)

        # --- Config row ---
        cfg = ttk.Frame(self)
        cfg.pack(fill="x", padx=16, pady=4)

        ttk.Label(cfg, text="Raw CSV:").grid(row=0, column=0, sticky="w")
        ttk.Entry(cfg, textvariable=self.csv_path, width=55).grid(row=0, column=1, padx=6)
        ttk.Button(cfg, text="Browse…", command=self._browse_csv).grid(row=0, column=2)

        ttk.Label(cfg, text="SQLite DB:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(cfg, textvariable=self.db_path, width=55).grid(row=1, column=1, padx=6, pady=(6, 0))
        ttk.Button(cfg, text="Browse…", command=self._browse_db).grid(row=1, column=2, pady=(6, 0))

        ttk.Button(cfg, text="▶ Run ETL Pipeline", style="Accent.TButton",
                   command=self._run_etl_threaded).grid(row=0, column=3, rowspan=2, padx=(20, 0), sticky="ns")

        # --- Tabs ---
        self.tabs = ttk.Notebook(self)
        self.tabs.pack(fill="both", expand=True, padx=16, pady=10)

        self.log_tab = ttk.Frame(self.tabs, style="Panel.TFrame")
        self.query_tab = ttk.Frame(self.tabs, style="Panel.TFrame")
        self.chart_tab = ttk.Frame(self.tabs, style="Panel.TFrame")

        self.tabs.add(self.log_tab, text="Pipeline Log")
        self.tabs.add(self.query_tab, text="SQL Explorer")
        self.tabs.add(self.chart_tab, text="Charts (Power BI-style)")

        self._build_log_tab()
        self._build_query_tab()
        self._build_chart_tab()

    # ------------------------------------------------------------------
    def _build_log_tab(self):
        self.log_text = tk.Text(self.log_tab, bg="#12131a", fg="#a6e3a1",
                                 insertbackground=TEXT, font=("Consolas", 10), wrap="word")
        self.log_text.pack(fill="both", expand=True, padx=10, pady=10)
        self._log("Ready. Click 'Run ETL Pipeline' to extract, clean, and load the data.")

    def _log(self, msg):
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")

    def _browse_csv(self):
        path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if path:
            self.csv_path.set(path)

    def _browse_db(self):
        path = filedialog.asksaveasfilename(defaultextension=".db", filetypes=[("SQLite DB", "*.db")])
        if path:
            self.db_path.set(path)

    def _run_etl_threaded(self):
        threading.Thread(target=self._run_etl, daemon=True).start()

    def _run_etl(self):
        csv_p = Path(self.csv_path.get())
        db_p = Path(self.db_path.get())

        if not csv_p.exists():
            messagebox.showerror("File not found", f"CSV not found:\n{csv_p}")
            return

        self.tabs.select(self.log_tab)
        self._log("\n--- Starting pipeline ---")
        try:
            run_pipeline(csv_p, db_p, log_callback=self._log)
            self._log("--- Pipeline finished successfully ---")
            self._refresh_table_list()
            messagebox.showinfo("Done", "ETL pipeline completed successfully.")
        except Exception as e:
            self._log(f"ERROR: {e}")
            messagebox.showerror("Pipeline failed", str(e))

    # ------------------------------------------------------------------
    def _build_query_tab(self):
        top = ttk.Frame(self.query_tab, style="Panel.TFrame")
        top.pack(fill="x", padx=10, pady=10)

        ttk.Label(top, text="Table:", style="Panel.TLabel").pack(side="left")
        self.table_choice = ttk.Combobox(top, values=[], width=20, state="readonly")
        self.table_choice.pack(side="left", padx=6)
        self.table_choice.bind("<<ComboboxSelected>>", lambda e: self._preview_table())

        ttk.Button(top, text="Refresh Tables", command=self._refresh_table_list).pack(side="left", padx=10)

        self.sql_box = tk.Text(self.query_tab, height=6, bg="#12131a", fg=TEXT,
                                insertbackground=TEXT, font=("Consolas", 10))
        self.sql_box.pack(fill="x", padx=10, pady=(0, 6))
        self.sql_box.insert("1.0", "SELECT * FROM fact_sales LIMIT 20;")

        ttk.Button(self.query_tab, text="▶ Run SQL", command=self._run_sql).pack(anchor="w", padx=10, pady=(0, 8))

        self.result_tree = ttk.Treeview(self.query_tab, show="headings")
        self.result_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    def _refresh_table_list(self):
        db_p = Path(self.db_path.get())
        if not db_p.exists():
            return
        conn = sqlite3.connect(db_p)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")]
        conn.close()
        self.table_choice["values"] = tables
        if tables:
            self.table_choice.set(tables[0])

    def _preview_table(self):
        table = self.table_choice.get()
        self.sql_box.delete("1.0", "end")
        self.sql_box.insert("1.0", f"SELECT * FROM {table} LIMIT 50;")
        self._run_sql()

    def _run_sql(self):
        db_p = Path(self.db_path.get())
        if not db_p.exists():
            messagebox.showwarning("No database", "Run the ETL pipeline first.")
            return
        query = self.sql_box.get("1.0", "end").strip()
        try:
            conn = sqlite3.connect(db_p)
            df = pd.read_sql_query(query, conn)
            conn.close()
        except Exception as e:
            messagebox.showerror("SQL error", str(e))
            return

        self.result_tree.delete(*self.result_tree.get_children())
        self.result_tree["columns"] = list(df.columns)
        for col in df.columns:
            self.result_tree.heading(col, text=col)
            self.result_tree.column(col, width=110, anchor="center")
        for _, row in df.iterrows():
            self.result_tree.insert("", "end", values=list(row))

    # ------------------------------------------------------------------
    def _build_chart_tab(self):
        btns = ttk.Frame(self.chart_tab, style="Panel.TFrame")
        btns.pack(fill="x", padx=10, pady=10)

        ttk.Button(btns, text="Revenue Trend", command=self._chart_revenue_trend).pack(side="left", padx=4)
        ttk.Button(btns, text="Top Products", command=self._chart_top_products).pack(side="left", padx=4)
        ttk.Button(btns, text="Regional Sales", command=self._chart_regional_sales).pack(side="left", padx=4)

        self.chart_holder = ttk.Frame(self.chart_tab, style="Panel.TFrame")
        self.chart_holder.pack(fill="both", expand=True, padx=10, pady=10)
        self.canvas = None

    def _query_df(self, sql):
        db_p = Path(self.db_path.get())
        if not db_p.exists():
            messagebox.showwarning("No database", "Run the ETL pipeline first.")
            return None
        conn = sqlite3.connect(db_p)
        df = pd.read_sql_query(sql, conn)
        conn.close()
        return df

    def _render_figure(self, fig):
        if self.canvas:
            self.canvas.get_tk_widget().destroy()
        self.canvas = FigureCanvasTkAgg(fig, master=self.chart_holder)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(fill="both", expand=True)

    def _chart_revenue_trend(self):
        df = self._query_df("""
            SELECT d.year || '-' || printf('%02d', d.month) AS period,
                   ROUND(SUM(f.revenue),2) AS revenue
            FROM fact_sales f JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY d.year, d.month ORDER BY d.year, d.month;
        """)
        if df is None or df.empty:
            return
        fig, ax = plt.subplots(figsize=(9, 5), facecolor=PANEL)
        ax.set_facecolor(PANEL)
        ax.plot(df["period"], df["revenue"], color=ACCENT, marker="o", linewidth=2)
        ax.set_title("Monthly Revenue Trend", color=TEXT)
        ax.tick_params(axis="x", rotation=60, colors=TEXT, labelsize=7)
        ax.tick_params(axis="y", colors=TEXT)
        for spine in ax.spines.values():
            spine.set_color(MUTED)
        fig.tight_layout()
        self._render_figure(fig)

    def _chart_top_products(self):
        df = self._query_df("""
            SELECT p.product_name, ROUND(SUM(f.revenue),2) AS revenue
            FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
            GROUP BY p.product_id ORDER BY revenue DESC LIMIT 10;
        """)
        if df is None or df.empty:
            return
        fig, ax = plt.subplots(figsize=(9, 5), facecolor=PANEL)
        ax.set_facecolor(PANEL)
        ax.barh(df["product_name"][::-1], df["revenue"][::-1], color=ACCENT)
        ax.set_title("Top 10 Products by Revenue", color=TEXT)
        ax.tick_params(colors=TEXT, labelsize=8)
        for spine in ax.spines.values():
            spine.set_color(MUTED)
        fig.tight_layout()
        self._render_figure(fig)

    def _chart_regional_sales(self):
        df = self._query_df("""
            SELECT r.region_name, ROUND(SUM(f.revenue),2) AS revenue
            FROM fact_sales f JOIN dim_region r ON f.region_id = r.region_id
            GROUP BY r.region_id ORDER BY revenue DESC;
        """)
        if df is None or df.empty:
            return
        fig, ax = plt.subplots(figsize=(7, 6), facecolor=PANEL)
        colors = plt.cm.Blues([0.9 - i * 0.13 for i in range(len(df))])
        ax.pie(df["revenue"], labels=df["region_name"], autopct="%1.1f%%",
               colors=colors, textprops={"color": TEXT})
        ax.set_title("Revenue Share by Region", color=TEXT)
        fig.tight_layout()
        self._render_figure(fig)


if __name__ == "__main__":
    app = SalesETLApp()
    app.mainloop()
