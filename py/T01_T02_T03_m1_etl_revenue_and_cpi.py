"""
資料工程模組一：宏觀名目營收與物價平減 ETL 管線 (T01_T02_T03_m1_etl_revenue_and_cpi.py)

【專題假說定位】
驗證「總體繁榮的貨幣幻覺」：透過經濟部餐飲業名目營業額指數 (T02) 與行政院主計總處
消費者物價指數 (T03) 進行平減除算 (T01)，剝離通膨價格因素，實證餐飲大盤帳面產值擴張
背後，實質消費量成長受限之宏觀基底。
"""

import sqlite3
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd

# 1. 動態相對路徑解析 (Dynamic Relative Path Resolution)
try:
    SCRIPT_DIR = Path(__file__).resolve().parent
except NameError:
    SCRIPT_DIR = Path.cwd()

BASE_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "py" else SCRIPT_DIR
SCRIPT_DIR = BASE_DIR / "py"

DATA_DIR = BASE_DIR / "資料來源" / "m1"
FINISHED_DIR = SCRIPT_DIR / "成品"
DB_PATH = SCRIPT_DIR / "catering_analysis.sqlite"
FINISHED_DIR.mkdir(parents=True, exist_ok=True)

# 2. 經濟部名目營收指數解析與 ISO 8601 時序標準化 (T02 Extraction & Normalization)
rev_file = DATA_DIR / "T02_m1_catering_revenue_monthly_raw.csv"

# 多重編碼容錯讀取機制 (Encoding-resilient Ingestion)
try:
    df_rev_raw = pd.read_csv(rev_file, encoding="utf-8")
except UnicodeDecodeError:
    df_rev_raw = pd.read_csv(rev_file, encoding="big5")

# 篩選餐飲業母體與營業額指數指標，剝除空白雜訊
cat_df = df_rev_raw[
    (df_rev_raw["行業別"].str.strip() == "餐飲業")
    & (df_rev_raw["統計項目"] == "營業額指數")
].copy()

# 民國紀年字串轉西元 ISO 8601 年月規格 (ROC to YYYY-MM)
def roc_to_ym(x):
    s = str(x).strip()
    return f"{int(s[:3]) + 1911}-{s[3:]}" if len(s) == 5 else f"{int(s[:2]) + 1911}-{s[2:]}"

cat_df["year_month"] = cat_df["資料期(民國年)"].apply(roc_to_ym)
cat_df["catering_revenue_index"] = pd.to_numeric(cat_df["統計值(指數)"], errors="coerce")
cat_df["data_status"] = "Official_Actual"

# 截取 2015-01 至 2024-12 基準時序視窗 (120 個月)
df_t02 = (
    cat_df[(cat_df["year_month"] >= "2015-01") & (cat_df["year_month"] <= "2024-12")][
        ["year_month", "catering_revenue_index", "data_status"]
    ]
    .sort_values("year_month")
    .reset_index(drop=True)
)

# 3. 主計總處 XML 特殊分類物價指數解析與長寬轉置 (T03 XML Parsing & Long-to-Wide Pivot)
cpi_file = DATA_DIR / "T03_m1_macro_cpi_monthly_raw.xml"
tree = ET.parse(cpi_file)
root = tree.getroot()

# XML 樹狀結構節點遍歷與特定屬性抽取
cpi_records = []
for obs in root.findall(".//Obs"):
    item = obs.findtext("Item")
    period = obs.findtext("TIME_PERIOD")
    val_type = obs.findtext("TYPE")
    val = obs.findtext("Item_VALUE")
    if val_type == "原始值" and period and val:
        cpi_records.append({
            "item": item,
            "year_month": period.replace("M", "-"),
            "val": float(val)
        })

df_cpi_parsed = pd.DataFrame(cpi_records)

# 執行樞紐分析轉置：將垂直長表重構為水平時間序列寬表
df_cpi_pivot = df_cpi_parsed.pivot_table(index="year_month", columns="item", values="val").reset_index()

# 指標映射與 2021 基期特徵欄位鎖定
df_t03 = pd.DataFrame({
    "year_month": df_cpi_pivot["year_month"],
    "cpi_total": df_cpi_pivot["總指數(不含食物)(指數基期：民國110年=100)"],
    "cpi_core_index": df_cpi_pivot["總指數(不含蔬果及能源)【即核心物價】(指數基期：民國110年=100)"],
    "cpi_food_related": df_cpi_pivot["總指數(不含蔬菜水果)(指數基期：民國110年=100)"],
    "data_status": "Official_Actual"
})
df_t03 = (
    df_t03[(df_t03["year_month"] >= "2015-01") & (df_t03["year_month"] <= "2024-12")]
    .sort_values("year_month")
    .reset_index(drop=True)
)

# 4. 宏觀名目與實質營收物價平減除算 (T01 Real vs Nominal Revenue Deflation)
# 以年月為鍵值執行內部關聯 (Inner Join)
df_t01 = pd.merge(
    df_t02[["year_month", "catering_revenue_index"]],
    df_t03[["year_month", "cpi_total", "cpi_food_related"]],
    on="year_month"
)

# 執行平減除算公式：(名目營收指數 / 食物類物價指數) * 100，還原實質物理消費量
df_t01["real_revenue_index_calc"] = round(
    (df_t01["catering_revenue_index"] / df_t01["cpi_total"]) * 100, 2
)
df_t01["food_real_revenue_index_calc"] = round(
    (df_t01["catering_revenue_index"] / df_t01["cpi_food_related"]) * 100, 2
)

# 5. 標準化產出與雙軌持久化落地 (Dual-track Persistence: Clean CSV & SQLite)
df_t01.to_csv(FINISHED_DIR / "T01_m1_chart_real_vs_nominal_revenue_clean.csv", index=False, encoding="utf-8-sig")
df_t02.to_csv(FINISHED_DIR / "T02_m1_catering_revenue_monthly_clean.csv", index=False, encoding="utf-8-sig")
df_t03.to_csv(FINISHED_DIR / "T03_m1_macro_cpi_monthly_clean.csv", index=False, encoding="utf-8-sig")

conn = sqlite3.connect(DB_PATH)
df_t01.to_sql("T01_m1_chart_real_vs_nominal_revenue_clean", conn, if_exists="replace", index=False)
df_t02.to_sql("T02_m1_catering_revenue_monthly_clean", conn, if_exists="replace", index=False)
df_t03.to_sql("T03_m1_macro_cpi_monthly_clean", conn, if_exists="replace", index=False)
conn.close()

print("模組一 (T01, T02, T03) 處理完成，各輸出 120 筆官方實際值資料。")