"""
資料工程模組二（前置作業）：經濟部餐飲業調查報告 PDF 批次標準化重命名器 (00_rename_m2_survey_pdfs.py)

【專題假說定位】
非結構化資料前置治理：
經濟部統計處歷年《餐飲業經營實況調查報告》之表冊編號存在跨年度漂移（如營運困境由表 29 變更為表 18、表 26），
且公務報告「出版年份（民國年）」與「實際調查統計年度（西元年）」存在 1 年之時間落差。
本腳本透過「表號特徵 + 出版年標記」之雙維度子字串匹配，將 17 份非標準命名之原始 PDF，
規範化重命名為具備嚴格時間戳記之實體檔案（_cost_structure_raw.pdf 與 _operating_obstacles_raw.pdf），
以確保下游模組二文字解析管線（T05/T06）具備確定性之檔案定位基礎。
"""

from pathlib import Path
from typing import List, Tuple

# 1. 動態相對路徑解析 (Dynamic Relative Path Resolution)
try:
    SCRIPT_DIR = Path(__file__).resolve().parent
except NameError:
    SCRIPT_DIR = Path.cwd()

BASE_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "py" else SCRIPT_DIR
DATA_DIR = BASE_DIR / "資料來源" / "m2"

# 2. 建立精準 17 份雙特徵對照表 (對齊公務出版年與實質統計年度)
# 結構：(匹配特徵清單 [表號關鍵字, 民國出版年標記], 標準化規範檔名)
RENAME_RULES: List[Tuple[List[str], str]] = [
    # T05: 成本結構 (表 9 系列，計 8 份；因官方調查週期缺漏 2016 與 2021 年)
    (["表9", "105"], "2015_m2_cost_structure_raw.pdf"),
    (["表9", "107"], "2017_m2_cost_structure_raw.pdf"),
    (["表9", "108"], "2018_m2_cost_structure_raw.pdf"),
    (["表9", "109"], "2019_m2_cost_structure_raw.pdf"),
    (["表9", "110"], "2020_m2_cost_structure_raw.pdf"),
    (["表9", "112"], "2022_m2_cost_structure_raw.pdf"),
    (["表9", "113"], "2023_m2_cost_structure_raw.pdf"),
    (["表9", "114"], "2024_m2_cost_structure_raw.pdf"),
    
    # T06: 營運困境 (歷年表號動態漂移系列，計 9 份)
    (["表29", "105"], "2016_m2_operating_obstacles_raw.pdf"),
    (["表18", "106"], "2017_m2_operating_obstacles_raw.pdf"),
    (["表26", "107"], "2018_m2_operating_obstacles_raw.pdf"),
    (["表24", "108"], "2019_m2_operating_obstacles_raw.pdf"),
    (["表24", "109"], "2020_m2_operating_obstacles_raw.pdf"),
    (["表21", "110"], "2021_m2_operating_obstacles_raw.pdf"),
    (["表18", "111"], "2022_m2_operating_obstacles_raw.pdf"),
    (["表25", "112"], "2023_m2_operating_obstacles_raw.pdf"),
    (["表25", "113"], "2024_m2_operating_obstacles_raw.pdf"),
]

def execute_batch_rename(target_dir: Path) -> None:
    """執行 PDF 檔案特徵比對與批次重命名作業，內建冪等性與衝突防護。"""
    if not target_dir.exists():
        raise FileNotFoundError(f"前置門禁中斷：查無目標資料夾目錄：{target_dir}")

    print(f"啟動批次重命名作業，目標目錄：{target_dir}")
    print("-" * 65)

    renamed_count = 0
    already_clean_count = 0

    # 遍歷資料夾下所有 PDF 實體檔案
    for pdf_path in target_dir.glob("*.pdf"):
        file_name = pdf_path.name
        
        # 冪等性防護 (Idempotency Guard)：已標準化之檔案直接略過，避免二次修改
        if file_name.endswith("_raw.pdf"):
            already_clean_count += 1
            continue
            
        matched = False
        for keywords, target_standard_name in RENAME_RULES:
            # 雙特徵子字串判定：表號與年份標記必須同時成立
            if all(kw in file_name for kw in keywords):
                destination_path = target_dir / target_standard_name
                
                # 衝突防護：若目標檔名已存在，避免未授權覆寫
                if destination_path.exists():
                    print(f"【檔案覆蓋警告】目標檔已存在，執行安全更新：{target_standard_name}")
                    destination_path.unlink()  # 移除舊目標檔以利更新
                
                pdf_path.rename(destination_path)
                print(f"【成功重命名】 {file_name} -> {target_standard_name}")
                renamed_count += 1
                matched = True
                break
        
        if not matched:
            print(f"【未匹配警告】未符合任何重命名規則之檔案：{file_name}")

    print("-" * 65)
    print(f"作業完成：新增重命名 {renamed_count} 份，維持標準檔名 {already_clean_count} 份。")

if __name__ == "__main__":
    execute_batch_rename(DATA_DIR)