"""
資料工程模組四：全國營業稅籍母體快照與月度歇業趨勢 ETL 管線 (T09_T10_m4_etl_registry_and_closings.py)

【專題假說定位】
確立「個體淘汰曲線與母體結構定錨」：
1. 彙整經濟部商工行政資料之住宿及餐飲業逐月歇業家數 (T09)，實證總體營收擴張下個體加速退場之趨勢。
2. 透過流式分塊 (Chunking) 技術解析財政部 171 萬筆營業稅籍母體，過濾行業代號 56 之純餐飲在營店家 (T10)，
   定錨全台免稅小規模營業人與應稅一般營業人之結構比例，作為下游模組五門檻建模之母體快照。
"""

import sqlite3
import pandas as pd
from pathlib import Path

# 1. 動態相對路徑解析 (Dynamic Relative Path Resolution)
try:
    SCRIPT_DIR = Path(__file__).resolve().parent
except NameError:
    SCRIPT_DIR = Path.cwd()

BASE_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "py" else SCRIPT_DIR
SCRIPT_DIR = BASE_DIR / "py"

DATA_DIR = BASE_DIR / "資料來源" / "m4"
FINISHED_DIR = SCRIPT_DIR / "成品"
DB_PATH = SCRIPT_DIR / "catering_analysis.sqlite"
FINISHED_DIR.mkdir(parents=True, exist_ok=True)
conn = sqlite3.connect(DB_PATH)

# 2. 經濟部商工行政月度歇業家數時序清洗 (T09 Extraction & Normalization)
closing_raw_file = DATA_DIR / "T09_m4_chart_catering_closings_monthly_raw.csv"
df_cls_raw = pd.read_csv(closing_raw_file)

if "年月別" in df_cls_raw.columns:
    # 民國紀年轉西元 ISO 8601 年月規格 (ROC Calendar to YYYY-MM)
    def parse_roc(val):
        s = str(val).strip()
        return f"{int(s[:3]) + 1911}-{s[3:]}" if len(s) == 5 else f"{int(s[:2]) + 1911}-{s[2:]}"
        
    df_cls_raw["year_month"] = df_cls_raw["年月別"].apply(parse_roc)
    df_cls_raw["catering_closings_count"] = pd.to_numeric(df_cls_raw["住宿及餐飲業家數"], errors="coerce")
    
    # 截取 2015-01 至 2024-12 基準時序視窗 (120 個月)
    df_t09 = (
        df_cls_raw[(df_cls_raw["year_month"] >= "2015-01") & (df_cls_raw["year_month"] <= "2024-12")][
            ["year_month", "catering_closings_count"]
        ]
        .dropna()
        .sort_values("year_month")
        .reset_index(drop=True)
    )
else:
    df_t09 = df_cls_raw

# T09 標準化產出與雙軌持久化儲存 (Clean CSV & SQLite)
df_t09.to_csv(FINISHED_DIR / "T09_m4_chart_catering_closings_monthly_clean.csv", index=False, encoding="utf-8-sig")
df_t09.to_sql("T09_m4_chart_catering_closings_monthly_clean", conn, if_exists="replace", index=False)

# 3. 財政部全國稅籍母體串流分塊載入與代碼純化 (T10 Stream Chunking & Code Filtering)
reg_raw_file = DATA_DIR / "T10_m4_raw_active_registry_snapshot_raw.csv"
catering_chunks = []
chunk_size = 50000  # 定義單次 I/O 記憶體安全閾值 (5 萬列)

# 實施串流分塊迭代：以純字串型態載入，杜絕統編與代碼前導零失真
for chunk in pd.read_csv(reg_raw_file, chunksize=chunk_size, low_memory=False, dtype=str):
    # 前置門禁過濾：剔除統編或設立日期缺失之雜訊列
    chunk = chunk.dropna(subset=["統一編號", "設立日期"]).copy()
    
    # 標準行業分類特徵萃取：剝離小數點雜訊，錨定第 56 大類（餐飲業）
    main_code = chunk["行業代號"].fillna("").str.split(".").str[0].str.strip()
    filtered = chunk[main_code.str.startswith("56")].copy()
    
    if not filtered.empty:
        filtered["設立日期"] = pd.to_numeric(filtered["設立日期"], errors="coerce")
        catering_chunks.append(filtered)

# 聚合精煉餐飲母體 DataFrame
df_t10 = pd.concat(catering_chunks, ignore_index=True)

# T10 標準化產出與雙軌持久化儲存 (Clean CSV & SQLite)
df_t10.to_csv(FINISHED_DIR / "T10_m4_active_registry_snapshot_clean.csv", index=False, encoding="utf-8-sig")
df_t10.to_sql("T10_m4_active_registry_snapshot_clean", conn, if_exists="replace", index=False)
conn.close()

print("模組四 (T09, T10) 稅籍母體與月度歇業序列處理完成！")