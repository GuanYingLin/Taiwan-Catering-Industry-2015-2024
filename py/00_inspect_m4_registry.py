"""
資料工程模組四（前置作業）：T10 原始稅籍資料結構檢驗器 (00_inspect_m4_registry.py)

【專案用意說明】
本腳本用於快速讀取並預覽財政部原始稅籍清冊 (Raw Data)。
由於原始檔案夾帶系統轉換雜訊（如：首列匯出日期、科學記號、非餐飲業種），
本腳本提供輕量（nrows=3）觀測窗口，用以驗證底層資料欄位，
並佐證後續 ETL 清洗腳本 (T09_T10_m4_etl_registry_and_closings.py) 
執行 dropna 與行業代號 56 過濾之必要性。
"""

from pathlib import Path
import pandas as pd

# 1. 動態相對路徑解析 (Dynamic Relative Path Resolution)
try:
    SCRIPT_DIR = Path(__file__).resolve().parent
except NameError:
    SCRIPT_DIR = Path.cwd()

BASE_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "py" else SCRIPT_DIR
RAW_FILE = BASE_DIR / "資料來源" / "m4" / "T10_m4_raw_active_registry_snapshot_raw.csv"

# 2. 前置門禁驗證與輕量結構探勘 (Pre-flight Validation & Schema Inspection)
# 前置門禁阻斷：驗證實體檔案就位狀態
if not RAW_FILE.exists():
    raise FileNotFoundError(f"找不到目標檔案，請確認路徑與檔名：{RAW_FILE}")

# 例外處理防護網：執行輕量採樣與結構輸出
try:
    print(f"正在讀取前 3 筆資料進行檢驗：{RAW_FILE.name}")
    # 型態安全解析：以純字串限制讀取前 3 列，避免記憶體負載與代碼前導零失真
    df_preview = pd.read_csv(RAW_FILE, nrows=3, dtype=str)
    
    print("\n【資料欄位結構】:")
    print(df_preview.columns.tolist())
    
    print("\n【前 3 筆資料預覽】:")
    print(df_preview.to_markdown(index=False))
except Exception as e:
    print(f"讀取失敗: {e}")