
from datetime import datetime , timedelta
import sqlite3 as sq3
import conf as cnf
from candle import Candle
from mcad import  mcad_analyze_result_object
from adx import adx_analyze_result_object 
from adx import Trend
import transactiontraiding as tt
import tools
# self.ctm = ctm
# self.ctmString = ctmString
# self.open = open
# self.high = high
# self.low = low
# self.close = close
# self.vol = vol

def get_last_mcad_result_from_database(symbol, period)-> mcad_analyze_result_object:
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT * FROM mcad_results_{symbol}_{period} ORDER BY timestamp DESC LIMIT 1")
        mcad_result = cursor.fetchone()
        if mcad_result:
            mcad_result = mcad_analyze_result_object(
            time=mcad_result[1],
            result=mcad_result[2]
            )
        cursor.close()
        conn.close()
        
        return mcad_result
    except Exception as e:
       print(e)
    
def adx_result_exists_in_database(symbol, period ,adx_result: adx_analyze_result_object , adx_trend : Trend) -> bool:
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()

        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS adx_results_{symbol}_{period} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            adx Text
            
     
        )
        ''')

        exists = False
        cursor.execute(f"SELECT * FROM adx_results_{symbol}_{period} WHERE timestamp = ? AND adx = ? ", (adx_result.time,f"{adx_result.result.value} {adx_result.result.name} {adx_trend.value} {adx_trend.name}"))
     
        if cursor.fetchone() is not None :
            exists = True
        cursor.close()
        conn.close()
        
        return exists
    except Exception as e:
        print(e)
def mcad_result_exists_in_database(symbol, period ,mcad_result: mcad_analyze_result_object) -> bool:
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()

        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS mcad_results_{symbol}_{period} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            macd Text
            
     
        )
        ''')

        exists = False
        cursor.execute(f"SELECT * FROM mcad_results_{symbol}_{period} WHERE timestamp = ? AND macd = ? ", (mcad_result.time,f"{mcad_result.result.value} {mcad_result.result.name}"))
        if cursor.fetchone() is not None :
            exists = True
        cursor.close()
        conn.close()
        
        return exists
    except Exception as e:
        print(e)
    
def save_adx_result_to_database(symbol, period, adx_result: adx_analyze_result_object , adx_trend : Trend):
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS adx_results_{symbol}_{period} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            adx Text
            
     
        )
        ''')
        
        cursor.execute(
            f"INSERT INTO adx_results_{symbol}_{period} (timestamp, adx) VALUES (?, ?)",
            (adx_result.time, f"{adx_result.result.value} {adx_result.result.name} {adx_trend}" )
        )
    
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
       print(e)
def save_mcad_result_to_database(symbol, period, mcad_result: mcad_analyze_result_object):
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS mcad_results_{symbol}_{period} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            macd Text
            
     
        )
        ''')
        
        cursor.execute(
            f"INSERT INTO mcad_results_{symbol}_{period} (timestamp, macd) VALUES (?, ?)",
            (mcad_result.time, f"{mcad_result.result.value} {mcad_result.result.name}" )
        )
    
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(e)

def clear_candles_table(symbol , period):
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute(f"DROP TABLE IF EXISTS candles_{symbol}_{period}")
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
       print(e)

def transaction_exists_in_database(symbol, period, opened_transaction: tt.TransactionTrading) -> bool:
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS transactions_{period} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            time INTEGER,
            buy_sell TEXT,
            open_close TEXT
        )
        ''')
        
        cursor.execute(f"SELECT * FROM transactions_{period} WHERE symbol = ? AND open_close = ?", 
                       (symbol,  opened_transaction.open_close))
        
        exists = cursor.fetchone() is not None
        
        cursor.close()
        conn.close()
        
        return exists
    except Exception as e:
       print(e)
def save_transaction_to_database(symbol, period , time , buy_sell , open_close):
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        sql = f'''
        CREATE TABLE IF NOT EXISTS transactions_{period} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            time INTEGER,
            buy_sell TEXT,
            open_close TEXT
        )
        '''
        cursor.execute(sql)
        
        cursor.execute(
            f"INSERT INTO transactions_{period} (symbol, time, buy_sell, open_close) VALUES (?, ?, ?, ?)",
            (symbol, time, buy_sell, open_close)
        )
    
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
       print(e)
def update_transaction_status(symbol, period, time, buy_sell, new_status):
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS transactions_{period} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            time INTEGER,
            buy_sell TEXT,
            open_close TEXT
        )
        ''')
        
        cursor.execute(f"UPDATE transactions_{period} SET open_close = ? WHERE symbol = ? AND buy_sell = ?", 
                       (new_status, symbol, buy_sell))
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
       print(e)
def get_transations_from_database(period, open_close):
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS transactions_{period} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            time INTEGER,
            buy_sell TEXT,
            open_close TEXT
        )
        ''')
        
        cursor.execute(f"SELECT * FROM transactions_{period} WHERE open_close = ?", (open_close,))
        transactions = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return transactions
    except Exception as e:
       print(e)

def get_signal_to_open_transaction(trade: str, symbol: str, status: str) -> bool:
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT,
            trade TEXT,
            symbol TEXT,
            status TEXT
        )
        ''')
        
        cursor.execute(f"SELECT * FROM signals WHERE trade = ? AND symbol = ? AND status = ?", 
                       (trade, symbol, status))
        
        exists = cursor.fetchone() is not None
        
        cursor.close()
        conn.close()
        
        return exists
    except Exception as e:
       print(e)
def update_signal_to_open_transaction(trade: str, symbol: str, status: str) -> None:
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT,
            trade TEXT,
            symbol TEXT,
            status TEXT
        )
        ''')
        cursor.execute(f"UPDATE signals SET status = ? WHERE trade = ? AND symbol = ?", 
                       (status, trade, symbol))
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
       print(e)
def set_signal_to_open_transaction(trade: str, symbol: str, status: str) -> None:
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT,
            trade TEXT,
            symbol TEXT,
            status TEXT
        )
        ''')
        # check if the signal already exists
        cursor.execute(f"SELECT * FROM signals WHERE trade = ? AND symbol = ? and status = ?", (trade, symbol, status))
        if cursor.fetchone() is not None:
           return False
        else:
            cursor.execute(
                f"INSERT INTO signals (time , trade, symbol, status) VALUES (?, ?, ?, ?)",
                (datetime.now(), trade, symbol, status)
            )

        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
       print(e)
def save_candle_to_database(symbol,period, candle : Candle):
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()

        sql = f'''
        CREATE TABLE IF NOT EXISTS candles_{symbol}_{period} (
            ctm INTEGER PRIMARY KEY,
            ctmString TEXT,
            open REAL,  
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER
        )
        '''
        cursor.execute(sql)
        
        
        cursor.execute(
            f"INSERT INTO candles_{symbol}_{period} (ctm, ctmString, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (candle.time,tools.int_to_datetime(candle.time) ,candle.open, candle.high, candle.low, candle.close, candle.tick_volume)
        )
    
        conn.commit()
        cursor.close()
        conn.close()
            #logger.info(f"Saved {len(candle)} candles for symbol {symbol} to database.")
    except Exception as e:
         print(e)
def get_product_from_database(symbol):
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        sql = f'''
                CREATE TABLE IF NOT EXISTS products (
                    symbol TEXT PRIMARY KEY,
                    name TEXT,
                    risk_price REAL,
                    SL REAL,
                    volume INTEGER
                )
                '''
        cursor.execute(sql)

        cursor.execute(f"SELECT * FROM products WHERE symbol = ?", (symbol,))
        product = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return product
    except Exception as e:
       print(e)
def save_product_to_database(symbol, name, risk_price, SL, volume):
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        sql = f'''
                CREATE TABLE IF NOT EXISTS products (
                    symbol TEXT PRIMARY KEY,
                    name TEXT,
                    risk_price REAL,
                    SL REAL,
                    volume INTEGER
                )
                '''
        cursor.execute(sql)
        
        cursor.execute(
            f"INSERT OR REPLACE INTO products (symbol, name, risk_price, SL, volume) VALUES (?, ?, ?, ?, ?)",
            (symbol, name, risk_price, SL, volume)
        )
    
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
       print(e)


def get_last_candle_from_database(symbol, period , countOfCandles) :
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT COUNT(*) FROM candles_{symbol}_{period}")
        max_count = cursor.fetchone()[0]
        if max_count < countOfCandles:
            countOfCandles = max_count

        cursor.execute(f"SELECT * FROM candles_{symbol}_{period} ORDER BY ctm DESC LIMIT {countOfCandles}")
        candles = cursor.fetchmany(countOfCandles)
       
        cursor.close()
        conn.close()
        
        return candles
    except Exception as e:
        print(e)  
def get_all_candles_from_database(symbol, period):
    try:
        conn = sq3.connect(cnf.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT * FROM candles_{symbol}_{period}")
        candles = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return candles
    except Exception as e:
        print(e)
