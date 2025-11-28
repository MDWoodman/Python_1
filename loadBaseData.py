from datetime import datetime, timedelta
import os
from symtable import Symbol
import time
#import login as login
import conf as cnf
import logging as logger
import tools as tools
from api_MT4 import API
from candle import Candle
from symbolx import SymbolX
import database as db


log_file_path = tools.logger_configuration()


# logger.basicConfig(filename=log_file_path, level=logger.INFO, 
#                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# api = API(cnf.username, cnf.password)

# session = api.login_xstation()
# print(str(session))

# logger.info(str(session)) 

# if session is None:
#     print('Login failed. No session returned.')
#     pass
# if session['status'] == False:
#     print('Login failed. Error code: {0}'.format(session['errorCode']))
#     pass

# # get ssId from login response
# ssid = session['streamSessionId']

def load_base_data(api: API):
   
 
    num_candles = 200
    start_time = None
    end_time = None
    period= tools.Period.from_string((cnf.PERIOD)).value
    multiplication = tools.calculate_multiplication_v2(cnf.PERIOD)
    for symbol in cnf.SYMBOLS_LIST:
        
         
        start_time = tools.get_start_time(num_candles, period)*1000
        end_time = tools.get_end_time()*1000
        candles_data = api.get_chart_range(symbol, period, start_time, end_time)

        print(f"Start: {start_time} ({tools.int_to_datetime(start_time)})")
        print(f"End: {end_time} ({tools.int_to_datetime(end_time)})")

        if candles_data :  
            rateInfos = candles_data#["_data"]
            print("Len : " + str(len(rateInfos)))
            db.clear_candles_table(symbol,period)
            candles = Candle.DeserialiazeCandels(rateInfos)
            for candle in candles:
                candle = tools.FormatCandles(candle)
                try:
                    db.save_candle_to_database(symbol,period, candle)
                except Exception as e:
                    print(f"Error: {e}")
                    logger.error(f"Error: {e} , Candle: {candle}")
                
                print(".")
           
        else :
            print(f"Failed to fetch candlestick data for {symbol}.")
            logger.error(f"Failed to fetch candlestick data for {symbol}.")
           
           
        

    return True       


print("Done")

