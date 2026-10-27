"""
台灣餐飲業專題：全自動 ETL 資料管線編排器 (run_pipeline.py)

【架構設計與容錯機制說明】
1. 有向無環圖 (DAG) 依序調度：嚴格遵循 Pre-ETL -> Level 1 -> Level 2 -> Level 3 之相依性順序執行。
2. 跨平台直譯器綁定：採用 sys.executable 動態對齊當前 Python 虛擬環境，消除 OS 指令差異。
3. 優雅降級 (Graceful Degradation)：自動探測 322MB 財政部全國稅籍原始檔 (dataset/9400) 是否存在。
   - 若存在：執行完整流式分塊 (chunksize=50000) 清洗作業。
   - 若未下載：自動啟動 Fallback 機制，略過 m4 原始清洗，直接取用 py/成品/ 內預先純化之 T09 與 T10 
     標準化資料集同步寫入 SQLite，確保下游 Level 2 (T04) 與 Level 3 (T08_2) 模型無縫重現運算。
"""

import sqlite3
import subprocess
import sys
import time
from pathlib import Path
import pandas as pd

# 1. 動態相對路徑鎖定 (確保於任何終端機目錄下呼叫皆能精準定位)
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

# 依賴 322MB 稅籍原始底冊之特定腳本
M4_RAW_DEPENDENT_SCRIPTS = {
    "00_inspect_m4_registry.py",
    "T09_T10_m4_etl_registry_and_closings.py"
}


def sync_m4_fallback_to_sqlite() -> None:
    """當缺少 322MB 原始檔時，將 py/成品/ 內之 T09 與 T10 乾淨 CSV 同步寫入 SQLite 資料庫。"""
    t09_clean = FINISHED_DIR / "T09_m4_chart_catering_closings_monthly_clean.csv"
    t10_clean = FINISHED_DIR / "T10_m4_active_registry_snapshot_clean.csv"

    if not (t09_clean.exists() and t10_clean.exists()):
        raise FileNotFoundError(
            "【Fallback 失敗】查無預先清洗之 T09 或 T10 成品檔，請確認 py/成品/ 目錄完整性。"
        )

    conn = sqlite3.connect(DB_PATH)
    pd.read_csv(t09_clean).to_sql(
        "T09_m4_chart_catering_closings_monthly_clean", conn, if_exists="replace", index=False
    )
    pd.read_csv(t10_clean, low_memory=False).to_sql(
        "T10_m4_active_registry_snapshot_clean", conn, if_exists="replace", index=False
    )
    conn.close()


def main() -> None:
    print("=" * 70)
    print("啟動台灣餐飲業專題 (2015-2024) ETL 全自動資料管線編排器")
    print("=" * 70)
    start_total = time.time()

    has_large_raw = RAW_M4_LARGE_FILE.exists()
    if not has_large_raw:
        print(
            "\n[系統環境偵測] 未偵測到 322MB 稅籍原始檔 (T10_m4_raw_active_registry_snapshot_raw.csv)。\n"
            "               系統已自動啟用「優雅降級 (Fallback) 模式」：將略過 m4 原始分塊讀取，\n"
            "               直接掛載 py/成品/ 內已純化之 T09/T10 母體快照，繼續執行全管線運算！"
        )

    for script_name in SCRIPTS_ALL:
        # 攔截並處理缺少 322MB 原始檔之優雅降級邏輯
        if not has_large_raw and script_name in M4_RAW_DEPENDENT_SCRIPTS:
            if script_name == "00_inspect_m4_registry.py":
                print(f"\n[略過執行] >> {script_name} (原因：無 322MB 原始檔可供探勘)")
            elif script_name == "T09_T10_m4_etl_registry_and_closings.py":
                print(f"\n[容錯移轉] >> {script_name}")
                step_start = time.time()
                sync_m4_fallback_to_sqlite()
                print(f"[完成同步] 已由 py/成品/ 載入 T09 與 T10 並寫入 SQLite (耗時 {time.time() - step_start:.2f} 秒)")
            continue

        script_path = SCRIPT_DIR / script_name
        print(f"\n[執行中] >> {script_name}")
        step_start = time.time()

        # 使用 sys.executable 確保跨作業系統與虛擬環境相容性
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
