"""
台灣餐飲業專題：全自動 ETL 資料管線編排器 (run_pipeline.py)

【架構設計與雙軌驗證機制】
1. 有向無環圖 (DAG) 依序調度：遵循 Pre-ETL -> Level 1 -> Level 2 -> Level 3 相依性順序執行。
2. 跨平台直譯器綁定：採用 sys.executable 動態對齊當前 Python 虛擬環境。
3. 互動式決策門禁 (Interactive Decision Gate)：針對未隨附於 Git 儲存庫之 322MB 稅籍原始底冊，
   提供「嚴格重現模式 (引導下載原始檔)」與「快速驗證模式 (Fallback 載入預洗成品)」雙軌選項。
"""

import sqlite3
import subprocess
import sys
import time
from pathlib import Path
import pandas as pd

# 1. 動態相對路徑鎖定
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "py" else SCRIPT_DIR
RAW_M4_LARGE_FILE = BASE_DIR / "資料來源" / "m4" / "T10_m4_raw_active_registry_snapshot_raw.csv"
FINISHED_DIR = SCRIPT_DIR / "成品"
DB_PATH = SCRIPT_DIR / "catering_analysis.sqlite"

# 2. 定義三階段標準 ETL 腳本清單
SCRIPTS_ALL = [
    # 階段 1：前置探查與標準化 (Pre-ETL)
    "00_rename_m2_survey_pdfs.py",
    "00_inspect_m4_registry.py",
    # 階段 2：基礎清洗模組 (Level 1)
    "T01_T02_T03_m1_etl_revenue_and_cpi.py",
    "T05_T06_m2_load_survey_data.py",
    "T07_m3_etl_minimum_wage.py",
    "T09_T10_m4_etl_registry_and_closings.py",
    # 階段 3：跨模組衍生模型與終端結算 (Level 2 & Level 3)
    "T04_m5_calc_pareto_stress_test.py",
    "T08_m6_parse_listed_groups_financials.py",
    "T08_m6_calc_capital_monopoly.py"
]

M4_RAW_DEPENDENT_SCRIPTS = {
    "00_inspect_m4_registry.py",
    "T09_T10_m4_etl_registry_and_closings.py"
}


def sync_m4_fallback_to_sqlite() -> None:
    """將 py/成品/ 內預先清洗之 T09 與 T10 標準化 CSV 寫入 SQLite 資料庫以供下游驗證。"""
    t09_clean = FINISHED_DIR / "T09_m4_chart_catering_closings_monthly_clean.csv"
    t10_clean = FINISHED_DIR / "T10_m4_active_registry_snapshot_clean.csv"

    if not (t09_clean.exists() and t10_clean.exists()):
        raise FileNotFoundError(
            "【門禁異常】查無預先清洗之 T09 或 T10 成品檔，請確認 py/成品/ 目錄完整性。"
        )

    conn = sqlite3.connect(DB_PATH)
    pd.read_csv(t09_clean).to_sql(
        "T09_m4_chart_catering_closings_monthly_clean", conn, if_exists="replace", index=False
    )
    pd.read_csv(t10_clean, low_memory=False).to_sql(
        "T10_m4_active_registry_snapshot_clean", conn, if_exists="replace", index=False
    )
    conn.close()


def prompt_missing_large_file() -> bool:
    """當偵測不到 322MB 原始檔時，啟動互動式門禁詢問執行者意圖。回傳 True 代表啟用 Fallback。"""
    print("\n" + "!" * 70)
    print("【資料工程門禁攔截】未偵測到模組四之 322MB 全國營業稅籍原始資料檔！")
    print("!" * 70)
    print("因該檔案超過 GitHub 100MB 單檔上限，故未直接隨附於儲存庫中。")
    print("若欲執行「100% 原始資料端到端重現」，請依下列規格手動部署：")
    print("  1. 官方下載網址：https://data.gov.tw/dataset/9400 (財政部全國營業稅籍登記資料集)")
    print("  2. 下載後解壓縮，將 CSV 重新命名為：T10_m4_raw_active_registry_snapshot_raw.csv")
    print(f"  3. 放置目標路徑：{RAW_M4_LARGE_FILE.parent}")
    print("-" * 70)

    # 支援指令列參數直接指定 (--fallback)
    if "--fallback" in sys.argv:
        print("[參數觸發] 偵測到 --fallback 旗標，自動切換為載入預洗成品模式。")
        return True

    print("請選擇後續執行模式：")
    print("  [1] 嚴格重現模式：立即中斷管線，待手動下載並放置 322MB 原始檔後再重新執行 (推薦完整驗證)")
    print("  [2] 快速驗證模式：略過模組四原始清洗，直接載入 py/成品/ 內之 T10 預洗快照，繼續驗證其餘 7 支腳本與下游模型")

    while True:
        choice = input("\n請輸入選項代碼 (1 或 2) 並按 Enter: ").strip()
        if choice == "1":
            print("\n[管線安全中斷] 請於部署原始檔案後，再次執行 python py/run_pipeline.py。")
            sys.exit(0)
        elif choice == "2":
            print("\n[切換快速驗證模式] 將使用預洗快照銜接下游管線...")
            return True
        else:
            print("輸入無效，請輸入數字 1 或 2。")


def main() -> None:
    print("=" * 70)
    print("啟動台灣餐飲業專題 (2015-2024) ETL 全自動資料管線編排器")
    print("=" * 70)

    has_large_raw = RAW_M4_LARGE_FILE.exists()
    use_fallback = False

    if not has_large_raw:
        use_fallback = prompt_missing_large_file()

    start_total = time.time()

    for script_name in SCRIPTS_ALL:
        if use_fallback and script_name in M4_RAW_DEPENDENT_SCRIPTS:
            if script_name == "00_inspect_m4_registry.py":
                print(f"\n[略過執行] >> {script_name} (快速驗證模式：無 322MB 原始檔可供探勘)")
            elif script_name == "T09_T10_m4_etl_registry_and_closings.py":
                print(f"\n[容錯移轉] >> {script_name}")
                step_start = time.time()
                sync_m4_fallback_to_sqlite()
                print(f"[完成同步] 已由 py/成品/ 載入 T09 與 T10 寫入 SQLite (耗時 {time.time() - step_start:.2f} 秒)")
            continue

        script_path = SCRIPT_DIR / script_name
        print(f"\n[執行中] >> {script_name}")
        step_start = time.time()

        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(SCRIPT_DIR),
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print(f"[錯誤中斷] {script_name} 執行失敗！錯誤訊息：\n{result.stderr}")
            sys.exit(1)

        if result.stdout.strip():
            for line in result.stdout.strip().split("\n"):
                print(f"  └─ {line}")
        print(f"[完成] 耗時 {time.time() - step_start:.2f} 秒")

    print("\n" + "=" * 70)
    print(f"管線全數執行完畢！總耗時：{time.time() - start_total:.2f} 秒")
    print(f"產出驗證：11 份標準化 _clean.csv 與 {DB_PATH.name} 關聯式資料庫已同步就緒。")
    print("=" * 70)


if __name__ == "__main__":
    main()
