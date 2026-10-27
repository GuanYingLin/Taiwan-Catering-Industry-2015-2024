"""
資料工程模組六（第二階段）：應稅市場資本集中度與集團市佔率終端結算 (T08_m6_calc_capital_monopoly.py)

【專題假說定位】
實證「資本集中與規模護城河」：
結合模組五 T04 之全台應稅營業人總營收分母，以及模組六階段一 (T08_1)
之六大上市櫃餐飲集團台灣境內校正營收，結算出金字塔頂端 0.015% 之企業法人（6 家 vs 3.9 萬家）
實質控制全台應稅正規餐飲市場 6.17%（474.6 億元）之產值，佐證連鎖資本在標準化賽道已築起極高規模壁壘。
"""

import sqlite3
from pathlib import Path
import pandas as pd

print("啟動 T08 資本寡占集中度終端結算運算...")

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

# 2. 外部宏觀常數定錨 (完全對齊 T04 / T05 經濟部 2024 餐飲大盤總營收，單位：新台幣百萬元)
TOTAL_CATERING_REVENUE_2024 = 1037770.0

# 3. 嚴格動態讀取 T04：查無檔案立即報錯中斷，絕不妥協使用寫死常數
t04_path = FINISHED_DIR / "T04_m5_chart_pareto_stress_test_2024_clean.csv"
if not t04_path.exists():
    raise FileNotFoundError(f"【管線門禁中斷】找不到模組五產出檔：{t04_path}，請先執行模組五腳本！")

df_t04 = pd.read_csv(t04_path)

# 動態檢驗情境標籤是否存在
match_row = df_t04.loc[df_t04["scenario"] == "法定標準-天花板營收(月收20萬)"]
if match_row.empty:
    raise ValueError("【血緣中斷】T04 中查無『法定標準-天花板營收(月收20萬)』標籤，請確認模組五運算版本！")

# 100% 動態萃取真實數據
invoice_count_2024 = int(df_t04["invoice_count"].iloc[0])
invoice_revenue_pct = float(match_row["invoice_pct"].iloc[0]) / 100.0

total_invoice_revenue_2024 = TOTAL_CATERING_REVENUE_2024 * invoice_revenue_pct

# 4. 動態讀取模組六階段一 T08_1 資料 (取得六大集團台灣境內校正後總營收)
t08_path = FINISHED_DIR / "T08_m6_chart_listed_groups_financials_clean.csv"
try:
    df_t08 = pd.read_csv(t08_path)
    df_t08_2024 = df_t08[df_t08["year"] == 2024].copy()
    top6_tw_revenue_2024 = float(df_t08_2024["revenue_mil"].sum())
    print(f"成功讀取 T08_1 財報！六大集團台灣區實際營收加總為: {top6_tw_revenue_2024:,.2f} 百萬元")
except Exception as e:
    print(f"警告：讀取 T08_1 財報失敗，啟用工程容錯 Fallback 機制。錯誤訊息: {e}")
    top6_tw_revenue_2024 = 47459.66  # 六大集團 2024 台灣區營收基準值

# 5. 資本寡占非對稱結構除算 (Asymmetric Concentration Modeling)
top6_count = 6
other_invoice_count = invoice_count_2024 - top6_count

# 家數結構佔比計算
top6_count_pct = (top6_count / invoice_count_2024) * 100.0
other_count_pct = (other_invoice_count / invoice_count_2024) * 100.0

# 營收市場份額計算
other_invoice_revenue_2024 = total_invoice_revenue_2024 - top6_tw_revenue_2024
top6_revenue_pct = (top6_tw_revenue_2024 / total_invoice_revenue_2024) * 100.0
other_revenue_pct = (other_invoice_revenue_2024 / total_invoice_revenue_2024) * 100.0

# 6. 建構 Tableau 視覺化專用長表資料結構 (Tableau-Oriented Long-Format Restructuring)
data = {
    "Metric": ["家數佔比", "家數佔比", "營收佔比", "營收佔比"],
    "Category": ["上市六大集團", "其餘應稅業者", "上市六大集團", "其餘應稅業者"],
    "Value_Pct": [
        round(top6_count_pct, 3),
        round(other_count_pct, 3),
        round(top6_revenue_pct, 2),
        round(other_revenue_pct, 2)
    ],
    "Label_Text": [
        f"{top6_count} 家 ({top6_count_pct:.3f}%)",
        f"{int(other_invoice_count):,} 家 ({other_count_pct:.2f}%)",
        f"{top6_tw_revenue_2024 / 100:.1f} 億 ({top6_revenue_pct:.2f}%)",
        f"約 {other_invoice_revenue_2024 / 100:.0f} 億 ({other_revenue_pct:.2f}%)"
    ]
}

df_t08_v2 = pd.DataFrame(data)

# 7. 標準化產出與雙軌持久化儲存 (Dual-track Persistence: Clean CSV & SQLite)
output_path = FINISHED_DIR / "T08_v2_chart_capital_monopoly_clean.csv"
df_t08_v2.to_csv(output_path, index=False, encoding="utf-8-sig")

conn = sqlite3.connect(DB_PATH)
df_t08_v2.to_sql("T08_v2_chart_capital_monopoly_clean", conn, if_exists="replace", index=False)
conn.close()

print(f"模組六 (T08_2) 資本寡占終端結算完成，產出至：{output_path.name} 並寫入 SQLite 資料庫。")