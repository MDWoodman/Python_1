import sqlite3
import os
from pathlib import Path

# generate parameters for login function
USERNAME = '52753886'
# USERNAME = '2470872'
PASSWORD = '!0Xm5XWDoQ1@fC'
# MODE = 'real'
MODE = 'demo'
MT5_SERVER = 'ICMarketsEU-Demo'
MT5_PATH = r'C:\Program Files\MetaTrader 5 IC Markets EU\terminal64.exe'

SYMBOLS_LIST = ['STOXX50' , 'UK100', 'USTEC', 'US30']
PERIOD = "M5"
NUM_CANDLES = 100
MAX_TIME_RESULT = 510
COOLDOWN_AFTER_SL_MIN = 45
AUTO_OPEN_TRANSACTION = True

# Dynamic SL/TP configuration.
# Available SL_METHOD values: ATR, SWING, SIGNAL_CANDLE
SL_METHOD = "SWING"
SL_ATR_PERIOD = 14
SL_ATR_MULTIPLIER = 1.5
SL_SWING_LOOKBACK = 10
SL_SIGNAL_BUFFER = 0.0
ENABLE_TP = False
TP_RR_RATIO = 2.0

# generate connection string for sqlite3 database
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_DB_PATH = _PROJECT_ROOT / "database" / "data" / "candels2.db"
_LEGACY_DB_PATH = Path(r"E:\sqlite\candels2.db")

_configured_db_path = os.getenv("DATABASE_PATH", "").strip()
if _configured_db_path:
	_candidate_db_path = Path(_configured_db_path).expanduser()
elif _LEGACY_DB_PATH.parent.exists():
	_candidate_db_path = _LEGACY_DB_PATH
else:
	_candidate_db_path = _DEFAULT_DB_PATH

try:
	_candidate_db_path.parent.mkdir(parents=True, exist_ok=True)
	DATABASE_PATH = str(_candidate_db_path)
except Exception:
	_DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
	DATABASE_PATH = str(_DEFAULT_DB_PATH)

API_HOST = ''
API_PORT = 443