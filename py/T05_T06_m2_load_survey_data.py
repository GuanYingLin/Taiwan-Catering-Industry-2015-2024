"""
資料工程模組二：餐飲業剛性成本結構與營運困境解析管線 (T05_T06_m2_load_survey_data.py)

【專題假說定位】
量化「結構性剛性成本壓縮」：
解析經濟部統計處歷年《餐飲業經營實況調查報告》原始 PDF。
Part A (T05)：萃取 2015–2024 年食材（銷貨成本）、人事與租金三大剛性成本佔比與營業利益率；
Part B (T06)：治理公務問卷跨年度表號漂移與欄位位移，萃取複選題營運困境時序變化，
實證底層小店在工資累退調升與原物料上漲夾擊下的系統性利潤侵蝕。
"""

import re
import sqlite3
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd
from pypdf import PdfReader

# 1. 動態相對路徑解析 (Dynamic Relative Path Resolution)
try:
    SCRIPT_DIR = Path(__file__).resolve().parent
except NameError:
    SCRIPT_DIR = Path.cwd()

BASE_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "py" else SCRIPT_DIR
SCRIPT_DIR = BASE_DIR / "py"
DATA_DIR = BASE_DIR / "資料來源" / "m2"
FINISHED_DIR = SCRIPT_DIR / "成品"
DB_PATH = SCRIPT_DIR / "catering_analysis.sqlite"
FINISHED_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------------
# Part A: 解析 T05 剛性成本結構 (共 8 份 PDF，官方欠缺 2016 與 2021 年)
# -------------------------------------------------------------
cost_structure_data: Dict[int, Dict[str, Any]] = {}

for pdf_path in DATA_DIR.glob("*.pdf"):
    if "cost_structure" in pdf_path.name:
        reader = PdfReader(pdf_path)
        text = reader.pages[0].extract_text()

        # 萃取調查問卷表頭記錄之「資料統計民國年」
        year_match = re.search(r"資料時間[：:]\s*(\d+)年", text) or re.search(r"(\d+)年", text)
        if not year_match:
            continue
        data_year = int(year_match.group(1)) + 1911

        for line in text.split("\n"):
            clean_line = line.strip()
            # 鎖定「56 餐飲業」之總計列
            if clean_line.startswith("56 餐飲業") or clean_line.startswith("56  餐飲業"):
                # 剔除破折號、填補符號與千分位逗號
                tokens = [t.replace(",", "") for t in clean_line.split() if t not in ["-", "—", ""]]
                numeric_tokens = [float(t) for t in tokens if re.match(r"^-?\d+(\.\d+)?$", t)]

                # 若開頭抓取到行業代號 56 則排除，確保指標對位精準
                if numeric_tokens and numeric_tokens[0] == 56:
                    numeric_tokens = numeric_tokens[1:]

                # 依公務統計表定欄位順序對齊：營收(0), 成本(2), 人事(3), 租金(4), 營業利益(-1)
                if len(numeric_tokens) >= 6:
                    cost_structure_data[data_year] = {
                        "year": data_year,
                        "revenue": int(numeric_tokens[0]),
                        "cogs": int(numeric_tokens[2]),
                        "wage_expense": int(numeric_tokens[3]),
                        "rent_expense": int(numeric_tokens[4]),
                        "operating_profit": int(numeric_tokens[-1]),
                        "data_status": "Official_Actual"
                    }
                break

# 嚴格對齊專題 10 年時間軸 (2015–2024)，實作缺失值對齊填充
t05_aligned_rows = []
for y in range(2015, 2025):
    if y in cost_structure_data:
        t05_aligned_rows.append(cost_structure_data[y])
    else:
        t05_aligned_rows.append({
            "year": y, "revenue": None, "cogs": None,
            "wage_expense": None, "rent_expense": None,
            "operating_profit": None, "data_status": "Missing_No_Data"
        })

df_t05 = pd.DataFrame(t05_aligned_rows).sort_values("year").reset_index(drop=True)

# 計算四大剛性成本與利潤佔比
df_t05["cogs_ratio"] = round(df_t05["cogs"] / df_t05["revenue"] * 100.0, 2)
df_t05["wage_ratio"] = round(df_t05["wage_expense"] / df_t05["revenue"] * 100.0, 2)
df_t05["rent_ratio"] = round(df_t05["rent_expense"] / df_t05["revenue"] * 100.0, 2)
df_t05["profit_margin"] = round(df_t05["operating_profit"] / df_t05["revenue"] * 100.0, 2)

# -------------------------------------------------------------
# Part B: 解析 T06 營運困境指標 (共 9 份 PDF，跨年份表號動態漂移治理)
# -------------------------------------------------------------
obstacles_data: Dict[int, Dict[str, Any]] = {}

for pdf_path in DATA_DIR.glob("*.pdf"):
    if "operating_obstacles" in pdf_path.name:
        reader = PdfReader(pdf_path)
        if len(reader.pages) < 2:
            continue
        page_0 = reader.pages[0].extract_text()
        page_1 = reader.pages[1].extract_text()

        # 萃取調查民國年
        year_match = re.search(r"資料時間[：:]\s*(\d+)年", page_0)
        if not year_match:
            continue
        roc_year = int(year_match.group(1))
        west_year = roc_year + 1911

        # 1. 第一頁 (Page 0)：定位「租金成本高」困境百分比
        rent_obstacle_pct = None
        for line in page_0.split("\n"):
            line_str = line.strip()
            if re.match(r"^56\s*餐", line_str):
                nums = [float(t) for t in line_str.split()[1:] if re.match(r"^\d+(\.\d+)?$", t)]
                if len(nums) >= 3:
                    rent_obstacle_pct = nums[2]  # 固定第三欄位
                break

        # 2. 第二頁 (Page 1)：跨年度表號漂移動態索引映射 (食材波動與人事困境)
        ingredient_obstacle_pct, wage_obstacle_pct = None, None
        for line in page_1.split("\n"):
            line_str = line.strip()
            if re.match(r"^56\s*餐", line_str):
                nums = [float(t) for t in line_str.split() if re.match(r"^\d+(\.\d+)?$", t)]

                if nums and nums[0] == 56:
                    nums = nums[1:]

                # 依據經濟部歷年問卷題號版本進行精準位移判定
                if roc_year in [105, 106]:
                    # 105-106 年：食材波動固定於索引 2；問卷尚未獨立拆分基本工資困境題
                    if len(nums) >= 3:
                        ingredient_obstacle_pct = nums[2]
                        wage_obstacle_pct = None
                elif roc_year in [107, 108, 109, 110]:
                    # 107-110 年問卷改版：食材波動移至索引 1，基本工資/人事困境移至索引 5
                    if len(nums) >= 6:
                        ingredient_obstacle_pct = nums[1]
                        wage_obstacle_pct = nums[5]
                elif roc_year in [111, 112, 113]:
                    # 111-113 年問卷精簡題目：人事困境往前位移至索引 4
                    if len(nums) >= 5:
                        ingredient_obstacle_pct = nums[1]
                        wage_obstacle_pct = nums[4]
                break

        if rent_obstacle_pct is not None:
            obstacles_data[west_year] = {
                "year": west_year,
                "obs_ingredient_fluctuation": ingredient_obstacle_pct,
                "obs_high_rent": rent_obstacle_pct,
                "obs_high_wage": wage_obstacle_pct
            }

df_t06 = pd.DataFrame([obstacles_data[y] for y in sorted(obstacles_data.keys())]).reset_index(drop=True)

# -------------------------------------------------------------
# Part C: 標準化產出與雙軌持久化儲存 (Clean CSV & SQLite)
# -------------------------------------------------------------
df_t05.to_csv(FINISHED_DIR / "T05_m2_chart_cost_structure_annual_clean.csv", index=False, encoding="utf-8-sig")
df_t06.to_csv(FINISHED_DIR / "T06_m2_chart_operating_obstacles_annual_clean.csv", index=False, encoding="utf-8-sig")

conn = sqlite3.connect(DB_PATH)
df_t05.to_sql("T05_m2_chart_cost_structure_annual_clean", conn, if_exists="replace", index=False)
df_t06.to_sql("T06_m2_chart_operating_obstacles_annual_clean", conn, if_exists="replace", index=False)
conn.close()

print("模組二 (T05, T06) 剛性成本與營運困境時序資料庫寫入完成。")