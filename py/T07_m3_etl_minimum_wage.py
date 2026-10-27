"""
資料工程模組三：法定最低工資歷史調升序列 ETL 管線 (T07_m3_etl_minimum_wage.py)

【專題假說定位】
錨定「制度性剛性人事底線」：清理勞動部基本工資制定歷程，藉由前向填充 (ffill) 展開
2015–2024 共 120 個月之月薪與時薪階梯序列 (T07)，實證基層勞動力成本在十年間大幅攀升，
形成微型餐飲無法透過內部管理規避之外部剛性支出。
"""

import re
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

DATA_DIR = BASE_DIR / "資料來源" / "m3"
FINISHED_DIR = SCRIPT_DIR / "成品"
DB_PATH = SCRIPT_DIR / "catering_analysis.sqlite"
FINISHED_DIR.mkdir(parents=True, exist_ok=True)

# 2. 雙重編碼容錯讀取 (Encoding-resilient Ingestion)
wage_file = DATA_DIR / "T07_m3_minimum_wage_monthly_raw.csv"

try:
    df_wage_raw = pd.read_csv(wage_file, encoding="utf-8")
except UnicodeDecodeError:
    df_wage_raw = pd.read_csv(wage_file, encoding="big5")

# 3. 非結構化文字之正規表示式數值萃取 (Regex Numerical Extraction)
df_wage_clean = df_wage_raw.copy()
df_wage_clean["effective_date"] = pd.to_datetime(
    df_wage_clean["實施日期（西元）"].astype(str), format="%Y%m%d", errors="coerce"
)

# 剝離千分位符號，並以 Regex 群組比對提取月薪與時薪純數值
df_wage_clean["min_wage_monthly"] = df_wage_clean["內容/調整金額（新台幣）"].apply(
    lambda x: float(re.search(r"月薪(\d+)", str(x).replace(",", "")).group(1))
    if re.search(r"月薪(\d+)", str(x).replace(",", ""))
    else None
)
df_wage_clean["min_wage_hourly"] = df_wage_clean["內容/調整金額（新台幣）"].apply(
    lambda x: float(re.search(r"時薪([\d.]+)", str(x).replace(",", "")).group(1))
    if re.search(r"時薪([\d.]+)", str(x).replace(",", ""))
    else None
)

# 4. 離散政策事件前向填充 (Event-driven Forward Fill)
df_wage_events = df_wage_clean.sort_values("effective_date").copy()
df_wage_events["min_wage_monthly"] = df_wage_events["min_wage_monthly"].ffill()
df_wage_events["min_wage_hourly"] = df_wage_events["min_wage_hourly"].ffill()

# 5. 連續時間網格生成與非同步時序對齊 (Time Grid Generation & As-of Merge)
# 建立 2015-01 至 2024-12 共 120 個月之基準連續日曆網格
date_range = pd.date_range(start="2015-01-01", end="2024-12-01", freq="MS")
df_monthly_grid = pd.DataFrame({"date": date_range, "year_month": date_range.strftime("%Y-%m")})

# 透過 merge_asof 向後追溯最近之生效工資，模擬法規剛性底線之連續延續效應
merged_wage = pd.merge_asof(
    df_monthly_grid.sort_values("date"),
    df_wage_events.dropna(subset=["effective_date"]).sort_values("effective_date")[
        ["effective_date", "min_wage_monthly", "min_wage_hourly"]
    ],
    left_on="date",
    right_on="effective_date",
    direction="backward"
)

# 型態標準化：補齊缺失並轉換為整數型態
df_t07 = (
    merged_wage[["year_month", "min_wage_monthly", "min_wage_hourly"]]
    .fillna(0)
    .astype({"min_wage_monthly": int, "min_wage_hourly": int})
)

# 6. 標準化成品與雙軌持久化儲存 (Dual-track Persistence: Clean CSV & SQLite)
df_t07.to_csv(FINISHED_DIR / "T07_m3_chart_minimum_wage_monthly_clean.csv", index=False, encoding="utf-8-sig")

conn = sqlite3.connect(DB_PATH)
df_t07.to_sql("T07_m3_chart_minimum_wage_monthly_clean", conn, if_exists="replace", index=False)
conn.close()

print("模組三 (T07) 基本工資調幅序列處理完成，共輸出 120 個月數據。")