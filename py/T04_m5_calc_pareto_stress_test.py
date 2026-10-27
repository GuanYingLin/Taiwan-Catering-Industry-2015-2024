"""
資料工程模組五：營業稅法門檻與實務區間情境壓力測試模型 (T04_m5_calc_pareto_stress_test.py)

【專題假說定位】
實證「微型餐飲生存邊界之數學鎖定」：
以營業稅法第 13 條免徵發票門檻（月收 20 萬元）為法定基準，建構「實務下限（月收 10 萬）」
與「實務上限黑數容差（月收 30 萬）」之三維情境壓力測試 (T04)。
實證即便在允許小規模營業人超額溢收 50% 之極端假設下，佔母體 73.84% 之免稅業者
最高僅能分得全台 38.75% 之總營收，數學上不可逆地確立 61.25% 至 87.08% 之
大盤產值牢牢集中於 26.16% 之應稅營業人，驗證帕累托極端寡占結構。
"""

import sqlite3
from pathlib import Path
import pandas as pd

# 1. 動態相對路徑解析 (Dynamic Relative Path Resolution)
try:
    SCRIPT_DIR = Path(__file__).resolve().parent
except NameError:
    SCRIPT_DIR = Path.cwd()

BASE_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "py" else SCRIPT_DIR
SCRIPT_DIR = BASE_DIR / "py"
FINISHED_DIR = SCRIPT_DIR / "成品"
DB_PATH = SCRIPT_DIR / "catering_analysis.sqlite"
FINISHED_DIR.mkdir(parents=True, exist_ok=True)

# 2. 載入模組四純化之在營餐飲母體快照 (T10 Ingestion)
t10_file = FINISHED_DIR / "T10_m4_active_registry_snapshot_clean.csv"
if not t10_file.exists():
    raise FileNotFoundError(f"前置門禁中斷：查無 T10 母體資料檔：{t10_file}")

df_t10 = pd.read_csv(t10_file, low_memory=False)

# 3. 時點快照截斷與體質結構定錨 (Temporal Censoring: Cutoff at 2024-12-31)
df_t10["設立日期_num"] = pd.to_numeric(df_t10["設立日期"], errors="coerce")
df_2024 = df_t10[df_t10["設立日期_num"] <= 1131231].copy()

# 統計免稅小規模營業人 (N) 與應稅一般營業人 (Y) 基底家數
base_exempt = int((df_2024["使用統一發票"] == "N").sum())
base_invoice = int((df_2024["使用統一發票"] == "Y").sum())
base_total = base_exempt + base_invoice

# 參數設定：經濟部統計處 2024 年餐飲業大盤實際總產值 (新台幣百萬元)
REV_2024_TOTAL = 1037770.0

# 4. 方案 S：法規與實務營收區間情境模擬運算 (Regulatory & Empirical Scenario Modeling)
# (1) 基準家數結構佔比
ex_base_pct = round(base_exempt / base_total * 100, 2)
inv_base_pct = round(100.0 - ex_base_pct, 2)

# (2) 實務下限情境：免稅店以月收 10 萬 (年收 1.20 百萬) 計
ex_lower_pct = round((base_exempt * 1.20 / REV_2024_TOTAL) * 100, 2)
inv_lower_pct = round(100.0 - ex_lower_pct, 2)

# (3) 法定標準天花板：免稅店全數以法定上限月收 20 萬 (年收 2.40 百萬) 計
ex_std_pct = round((base_exempt * 2.40 / REV_2024_TOTAL) * 100, 2)
inv_std_pct = round(100.0 - ex_std_pct, 2)

# (4) 實務上限情境 (黑數容差測試)：免稅店以月收 30 萬 (年收 3.60 百萬) 計
ex_upper_pct = round((base_exempt * 3.60 / REV_2024_TOTAL) * 100, 2)
inv_upper_pct = round(100.0 - ex_upper_pct, 2)

# 5. 建構結構化情境分析資料集 (Scenario Dataset Construction)
t04_data = [
    {
        "scenario": "基準-母體家數結構",
        "exempt_pct": ex_base_pct,
        "invoice_pct": inv_base_pct,
        "exempt_count": base_exempt,
        "invoice_count": base_invoice,
        "note": f"2024 年底在營餐飲母體 (免稅小規模:{base_exempt:,} 家 / 應稅一般營業人:{base_invoice:,} 家)"
    },
    {
        "scenario": "實務下限-常態營收(月收10萬)",
        "exempt_pct": ex_lower_pct,
        "invoice_pct": inv_lower_pct,
        "exempt_count": base_exempt,
        "invoice_count": base_invoice,
        "note": f"以微型小店實務營運下限推估，應稅業者掌控 {inv_lower_pct}% 大盤產值"
    },
    {
        "scenario": "法定標準-天花板營收(月收20萬)",
        "exempt_pct": ex_std_pct,
        "invoice_pct": inv_std_pct,
        "exempt_count": base_exempt,
        "invoice_count": base_invoice,
        "note": f"依據營業稅法第 13 條上限 20 萬元推估，應稅業者法定保底囊括 {inv_std_pct}% 產值"
    },
    {
        "scenario": "實務上限-黑數容差(月收30萬)",
        "exempt_pct": ex_upper_pct,
        "invoice_pct": inv_upper_pct,
        "exempt_count": base_exempt,
        "invoice_count": base_invoice,
        "note": f"容許小店溢收 50% 逃漏黑數推估，應稅業者依然牢固掌控 {inv_upper_pct}% 產值"
    }
]

df_t04 = pd.DataFrame(t04_data)

# 6. 標準化產出與雙軌持久化儲存 (Dual-track Persistence: Clean CSV & SQLite)
df_t04.to_csv(FINISHED_DIR / "T04_m5_chart_pareto_stress_test_2024_clean.csv", index=False, encoding="utf-8-sig")

conn = sqlite3.connect(DB_PATH)
df_t04.to_sql("T04_m5_chart_pareto_stress_test_2024_clean", conn, if_exists="replace", index=False)
conn.close()

print("模組五 (T04) 方案 S 門檻壓力測試計算完成。")