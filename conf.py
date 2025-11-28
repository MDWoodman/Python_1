import sqlite3

# generate parameters for login function
USERNAME = '17712595'
# USERNAME = '2470872'
PASSWORD = 'M820824d!'
# MODE = 'real'
MODE = 'demo'

SYMBOLS_LIST = ['USDJPY' , 'EURUSD', 'GBPUSD', 'AUDUSD']
PERIOD = "M15"
NUM_CANDLES = 100
MAX_TIME_RESULT = 510

# generate connection string for sqlite3 database
DATABASE_PATH = 'E:\sqlite\candels.db'

API_HOST = ''
API_PORT = 443