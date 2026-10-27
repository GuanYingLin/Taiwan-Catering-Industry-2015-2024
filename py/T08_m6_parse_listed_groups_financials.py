"""
資料工程模組六（第一階段）：六大上市櫃餐飲集團財報解析與境內權重校正 (T08_m6_parse_listed_groups_financials.py)

【專題假說定位】
量化「資本規模經濟與利潤防線」：
解析公開資訊觀測站 (MOPS) 六大指標性上市櫃餐飲集團之合併綜合損益表 HTML。
透過非結構化標籤剝離演算法萃取營業收入、營業成本與營業利益，並導入台灣事業群營收權重，
精確剝離跨國海外子公司貢獻，以對齊國內稅籍母體分母，實證資本集團在食材與工資通膨下的規模防禦力。
"""

import re
import sqlite3
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd

# 1. 動態相對路徑解析 (Dynamic Relative Path Resolution)
try:
    SCRIPT_DIR = Path(__file__).resolve().parent
except NameError:
    SCRIPT_DIR = Path.cwd()

BASE_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "py" else SCRIPT_DIR
SCRIPT_DIR = BASE_DIR / "py"
DATA_DIR = BASE_DIR / "資料來源" / "m6"
FINISHED_DIR = SCRIPT_DIR / "成品"
DB_PATH = SCRIPT_DIR / "catering_analysis.sqlite"
FINISHED_DIR.mkdir(parents=True, exist_ok=True)

# 2. 研究樣本清單與賽道定位矩陣
TARGET_COMPANIES: List[Dict[str, str]] = [
    {"code": "2727", "name": "王品", "file": "2024_2727_wowprime_income_statement_raw.html", "segment": "大眾連鎖聚餐", "task": "百億規模採購議價優勢"},
    {"code": "2753", "name": "八方雲集", "file": "2024_2753_bafang_income_statement_raw.html", "segment": "百元庶民小吃", "task": "中央工廠標準化抗通膨韌性"},
    {"code": "7705", "name": "三商餐飲", "file": "2024_7705_mercuries_income_statement_raw.html", "segment": "國民平價連鎖", "task": "長壽品牌對基層工資調升承受力"},
    {"code": "2729", "name": "瓦城", "file": "2024_2729_ttfb_income_statement_raw.html", "segment": "商場主題餐廳", "task": "品牌溢價抵禦百貨抽成與租金"},
    {"code": "1259", "name": "安心餐飲", "file": "2024_1259_anshin_income_statement_raw.html", "segment": "連鎖速食(摩斯)", "task": "直營重資產受基本時薪調升擠壓"},
    {"code": "1268", "name": "漢來美食", "file": "2024_1268_hilai_income_statement_raw.html", "segment": "高端精緻百匯", "task": "高客單價商務宴會轉嫁力"}
]

# 台灣事業群營收權重 (分部財務報告權重校正，對齊國內稅籍母體)
TW_WEIGHTS: Dict[str, float] = {
    "2727": 0.78,  # 王品：扣除中國大陸事業群
    "2753": 0.80,  # 八方雲集：扣除美國及海外門市
    "7705": 1.00,  # 三商餐飲：100% 台灣本土內需
    "2729": 0.95,  # 瓦城：扣除少數海外事業
    "1259": 0.95,  # 安心餐飲：扣除澳洲與中國海外投資
    "1268": 0.98   # 漢來美食：以台灣本土據點為主
}

def parse_mops_income_statement(html_path: Path) -> Dict[int, Dict[str, float]]:
    """以輕量正規表達式解析 MOPS 綜合損益表 HTML，提取 2023 與 2024 會計科目數值。"""
    with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    # 擷取所有表格列 <tr> 區塊
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", content, re.DOTALL)
    item_map: Dict[str, tuple] = {}

    for row in rows:
        # 標籤剝離：去除所有 HTML Tag、&nbsp; 實體與頭尾空白
        cells = [
            re.sub(r"<[^>]+>", "", c).replace("&nbsp;", "").strip()
            for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.DOTALL)
        ]
        # MOPS 標準格式：[科目名稱, 113年金額, 113年%, 112年金額, 112年%]
        if len(cells) >= 5:
            accounting_item = cells[0]
            val_113 = cells[1].replace(",", "").strip()
            val_112 = cells[3].replace(",", "").strip()
            item_map[accounting_item] = (val_113, val_112)

    def extract_value(key: str, is_2024: bool) -> float:
        val_tuple = item_map.get(key, ("0", "0"))
        raw_val = val_tuple[0] if is_2024 else val_tuple[1]
        try:
            return float(raw_val)
        except ValueError:
            return 0.0

    parsed_annual_data = {}
    for year, is_2024 in [(2024, True), (2023, False)]:
        # 原始數值單位為新台幣千元，除以 1,000 轉換為「百萬元」
        rev = extract_value("營業收入合計", is_2024) / 1000.0
        cogs = extract_value("營業成本合計", is_2024) / 1000.0
        gross = (extract_value("營業毛利（毛損）淨額", is_2024) or extract_value("營業毛利（毛損）", is_2024)) / 1000.0
        op = extract_value("營業利益（損失）", is_2024) / 1000.0
        ni = (extract_value("母公司業主（淨利∕損）", is_2024) or extract_value("本期淨利（淨損）", is_2024)) / 1000.0

        parsed_annual_data[year] = {
            "rev": rev, "cogs": cogs, "gross": gross, "op": op, "ni": ni,
            "gm": round((gross / rev * 100.0), 2) if rev else 0.0,
            "om": round((op / rev * 100.0), 2) if rev else 0.0,
            "nm": round((ni / rev * 100.0), 2) if rev else 0.0
        }

    return parsed_annual_data

# 3. 批次解析與境內營收權重加權演算
records = []
for comp in TARGET_COMPANIES:
    html_file = DATA_DIR / comp["file"]
    if not html_file.exists():
        raise FileNotFoundError(f"前置門禁中斷：查無企業財報實體檔案：{html_file}")

    financial_data = parse_mops_income_statement(html_file)
    weight = TW_WEIGHTS.get(comp["code"], 1.0)

    for year in [2023, 2024]:
        metrics = financial_data[year]
        records.append({
            "year": year,
            "company_code": comp["code"],
            "company_name": comp["name"],
            "market_segment": comp["segment"],
            "research_task": comp["task"],
            "tw_revenue_weight": weight,
            "revenue_mil": round(metrics["rev"] * weight, 2),
            "cogs_mil": round(metrics["cogs"] * weight, 2),
            "gross_profit_mil": round(metrics["gross"] * weight, 2),
            "operating_profit_mil": round(metrics["op"] * weight, 2),
            "net_income_mil": round(metrics["ni"] * weight, 2),
            "gross_margin_pct": metrics["gm"],
            "operating_margin_pct": metrics["om"],
            "net_margin_pct": metrics["nm"]
        })

df_t08 = pd.DataFrame(records)

# 4. 標準化產出與雙軌持久化儲存 (Clean CSV & SQLite)
out_csv = FINISHED_DIR / "T08_m6_chart_listed_groups_financials_clean.csv"
df_t08.to_csv(out_csv, index=False, encoding="utf-8-sig")

conn = sqlite3.connect(DB_PATH)
df_t08.to_sql("T08_m6_chart_listed_groups_financials_clean", conn, if_exists="replace", index=False)
conn.close()

print(f"模組六 (T08_1) 上市集團財報解析完成，共處理 {len(records)} 筆年度觀測記錄。")