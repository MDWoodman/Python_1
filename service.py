from datetime import datetime, timedelta
import os
from symtable import Symbol
import time

from psycopg import Transaction
#import login as login
import conf as cnf
import logging as logger
import tools as tools
import api_xtb as api
import api_MT4 as api_mt4
from candle import Candle
from symbolx import SymbolX
import database as db
import mcad as mcad
import adx as adx
from loadBaseData import load_base_data
import asyncio
from datetime import datetime
from product_conf import ProductConf, ProductDB
import result as global_result
import ichi as ichi
import pprint as pp
import transactiontraiding as tt
import external_communication as external_communication

load_current_data = True

log_file_path = tools.logger_configuration()


# logger.basicConfig(filename=log_file_path, level=logger.INFO, 
#                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

api = api_mt4.API()#(cnf.username, cnf.password)

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

async def main():
   
    if load_current_data:
         result = load_base_data(api)

         if result == False:
             print("Error loading base data")
             logger.error("Error loading base data")
             return 
    products_list = ProductConf.load_products_from_json(os.path.dirname(os.path.abspath(__file__)) + '\\products.json')
    opened_transactions_list = []
    num = 0
    while True:
 
        num_candles = 1
        start_time = None
        end_time = None
        period= tools.Period.from_string((cnf.PERIOD)).value
        multiplication = tools.calculate_multiplication_v2(cnf.PERIOD)
        num = num+1
        run = False
        print("Num : " + str(num))
        if num > 10:
            run = True
            num = 0
        if run == True:
          for symbol in cnf.SYMBOLS_LIST:
            
            print("Processing symbol: " + symbol)
            product = ProductConf.find_product_by_symbol(products_list, symbol)
            if product is None:
                print(f"Product not found for symbol {symbol}")
                continue

            if load_current_data:
                start_time = tools.get_start_time(num_candles, period)*1000
            
            candles_data = api.get_last_candle(symbol , period)#get_chart_last(symbol, period, start_time)
            if candles_data : 
                rateInfos = candles_data#candles_data["rateInfos"]
            
                candles = Candle.DeserialiazeCandel(rateInfos)
                candle = tools.FormatCandles(candles)#[0])
                try:
                   db.save_candle_to_database(symbol,period, candle)
                except Exception as e:
                    print(f"Error: {e}")
                    logger.error(f"Error: {e} , Candle: {candle}")
                
                print(candle)
            else:
                print("Failed to fetch candlestick data.")
            
            #data = db.get_all_candles_from_database(symbol, period)
            data = db.get_last_candle_from_database(symbol, period, cnf.NUM_CANDLES)
            candles = []
            for d in data:
                candle = Candle(d[0], d[2], d[3], d[4], d[5], d[6])
                candles.append(candle)
            last_candle = candles[0]
            
            if len(candles) < cnf.NUM_CANDLES:
                print(f"Expected {cnf.NUM_CANDLES} candels for symbol {symbol}, got {len(candles)}")
                
            candles.reverse() 

            #ADX
            adx_analyze_result_obj = analyze_adx_candles(candles , product)
            if adx_analyze_result_obj[0] != None :
                print(f"Time adx result {tools.int_to_datetime( adx_analyze_result_obj[0].get_time())}")
                if(adx_analyze_result_obj[0].get_result() != adx.adx_result_enum.Boczny) :
                    print(f"ADX Data godzina : {datetime.now() } , Symbol {symbol} , Result {adx_analyze_result_obj[0].get_result()} , Time {tools.int_to_datetime(adx_analyze_result_obj[0].get_time())}")
                else:
                    print(f"ADX Data godzina : {datetime.now() } , Symbol {symbol}")

            #MCAD
            mcad_analyze_result_obj = analyze_mcad_candles(candles , product)
            if mcad_analyze_result_obj != None :
                print(f"Time mcad result {tools.int_to_datetime(mcad_analyze_result_obj.get_time())}")
                if(mcad_analyze_result_obj.get_result() != mcad.mcad_result_enum.Boczny) :
                    print(f"MCAD Data godzina : {datetime.now() } , Symbol {symbol} , Result {mcad_analyze_result_obj.get_result()} , Time {tools.int_to_datetime(mcad_analyze_result_obj.get_time())}")
                else:
                    print(f"MCAD Data godzina : {datetime.now() } , Symbol {symbol}")

            #ICHIMOKU
            ichimoku_result_K , ichimoku_result_S = analzye_ichimoku_candles(candles , product)
           
            

            analaze_result_obj = global_result.AnalysisResult(adx_analyze_result_obj , mcad_analyze_result_obj , ichimoku_result_K , ichimoku_result_S)

            diff_time_result =  analaze_result_obj.get_time_difference()
            result_K_result , result_S_result = analaze_result_obj.get_result()

            time_and_result_K , time_and_result_S = analaze_result_obj.get_time_and_result(diff_time_result, result_K_result , result_S_result)
            max_K = tools.get_max_time_from_list(time_and_result_K)
            max_S = tools.get_max_time_from_list(time_and_result_S)

            if len(time_and_result_K) > 0 and max_K > 0 and max_K < max_S:
                max = tools.get_max_time_from_list(time_and_result_K)
               
                if max > 0 and max < cnf.MAX_TIME_RESULT:
                    print(f"Time K result {tools.int_to_datetime(max)}")
                    print(f"Time K result {max}")
                    
                    if external_communication.check_if_transaction_is_opened("BUY", symbol) == False:
                        external_communication.send_signal_to_open_transaction("BUY", symbol)
                        if external_communication.check_get_signal_to_open_transaction("BUY", symbol):
                            external_communication.update_signal_to_open_transaction("BUY", symbol , "OPENED")
                            opened_transaction = tt.TransactionTrading(symbol , cnf.PERIOD , datetime.now() , "BUY" , "OPEN")
                            already_opened = tools.transaction_already_opened(opened_transactions_list, opened_transaction)
                        
                            if already_opened == False :
                                opened_transactions_list.append(opened_transaction)
                                db.save_transaction_to_database(symbol, cnf.PERIOD, datetime.now(), "BUY" , "OPEN")

                   
                else:
                    print("No time K result")
                pass

            if len(time_and_result_S) > 0 and max_S > 0 and max_S < max_K:
                max = tools.get_max_time_from_list(time_and_result_S)
                if max > 0 and max < cnf.MAX_TIME_RESULT:
                    print(f"Time S result {tools.int_to_datetime(max)}")
                    print(f"Time S result {max}")

                    # Check if there is already an opened transaction for this symbol
                    if external_communication.check_if_transaction_is_opened("SELL", symbol) == False:
                        external_communication.send_signal_to_open_transaction("SELL", symbol)
                        if external_communication.check_get_signal_to_open_transaction("SELL", symbol):
                            external_communication.update_signal_to_open_transaction("SELL", symbol , "OPENED")

                            opened_transaction = tt.TransactionTrading(symbol , cnf.PERIOD , datetime.now() , "SELL" , "OPEN")
                            already_opened = tools.transaction_already_opened(opened_transactions_list, opened_transaction)
                    
                            if already_opened == False :
                                opened_transactions_list.append(opened_transaction)
                                db.save_transaction_to_database(symbol, cnf.PERIOD, datetime.now(), "SELL" , "OPEN")
                else:
                    print("No time S result")
                pass

            for opened_transaction in opened_transactions_list:
                if opened_transaction.status == "OPEN":
                    if opened_transaction.get_type() == "BUY":
                        if check_adx_to_close_transaction(candles, product, opened_transaction) and check_mcad_to_close_transaction(candles, product, opened_transaction):
                            print(f"Closing BUY transaction for symbol {opened_transaction.get_symbol()} at {datetime.now()}")
                            external_communication.send_signal_to_close_transaction("BUY", opened_transaction.get_symbol())
                            if external_communication.check_get_signal_to_close_transaction("BUY", opened_transaction.get_symbol()):
                                opened_transaction.set_status("CLOSE")
                                db.update_transaction_status(opened_transaction.get_symbol(), cnf.PERIOD, opened_transaction.get_time(), "BUY", "CLOSE")
                           
                    elif opened_transaction.get_type() == "SELL":
                        if check_mcad_to_close_transaction(candles, product, opened_transaction) and check_adx_to_close_transaction(candles, product, opened_transaction):
                            print(f"Closing SELL transaction for symbol {opened_transaction.get_symbol()} at {datetime.now()}")
                            opened_transaction.set_status("CLOSE")
                            db.update_transaction_status(opened_transaction.get_symbol(), cnf.PERIOD, opened_transaction.get_time(), "SELL", "CLOSE")
                            external_communication.send_signal_to_close_transaction("SELL", opened_transaction.get_symbol())
            
          time.sleep(10) # Sleep for 1 minutes before the next iteration
        time.sleep(30)  # Sleep for 5 minute before the next iteration

def analzye_ichimoku_candles(candles:list[Candle] , product : ProductConf) -> ichi.ichimoku_analyze_result_object:
    last_n_candles = candles[-30:]

    ichi_obj = ichi.ichimoku_object()
    ichi_data = ichi_obj.get_data_from_candle_array(candles)
    ichi_result  = ichi_obj.analyze_ichimoku(ichi_data ,last_n_candles ,  product.tenkansen_period, product.kijunsen_period , product.senkouspan_period)
    ichi_analyze_result_obj = ichi.ichimoku_analyze_result_object(datetime.now(),product.symbol, cnf.PERIOD, ichi_result )
    result_K = []
    result_S = []
    # wzrostowo kupno
    if ichi_analyze_result_obj.get_result().crossover_result_tenkansen_kiusen == ichi.ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_gory  :
        result_K.append(str(ichi_analyze_result_obj.get_result().crossover_result_tenkansen_kiusen) + " ," + str(ichi_analyze_result_obj.get_result().time_of_cross_tenkansen_kiusen) +" , " + str(tools.int_to_datetime(ichi_analyze_result_obj.get_result().time_of_cross_tenkansen_kiusen)))
    if ichi_analyze_result_obj.get_result().crossover_result_price_kiusen == ichi.ichi_crossover_price_kiusen_result_enum.Przeciecie_do_gory : 
        result_K.append(str(ichi_analyze_result_obj.get_result().crossover_result_price_kiusen) + " ," + str(ichi_analyze_result_obj.get_result().time_of_cross_price_kiusen) +" , " + str(tools.int_to_datetime(ichi_analyze_result_obj.get_result().time_of_cross_price_kiusen)))
    if ichi_analyze_result_obj.get_result().crossover_price_senokuspan == ichi.ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_gory  :
        result_K.append(str(ichi_analyze_result_obj.get_result().crossover_price_senokuspan) + " ," + str(ichi_analyze_result_obj.get_result().time_of_cross_price_senokuspan) +" , " + str(tools.int_to_datetime(ichi_analyze_result_obj.get_result().time_of_cross_price_senokuspan)))
    if ichi_analyze_result_obj.get_result().crossover_price_senokuspan ==  ichi.ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_gory  :
        result_K.append(str(ichi_analyze_result_obj.get_result().crossover_price_senokuspan) + " ," + str(ichi_analyze_result_obj.get_result().time_of_cross_price_senokuspan) +" , " + str(tools.int_to_datetime(ichi_analyze_result_obj.get_result().time_of_cross_price_senokuspan)))

    # spadkowo sprzedaz
    if ichi_analyze_result_obj.get_result().crossover_result_tenkansen_kiusen == ichi.ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_dolu :
        result_S.append(str(ichi_analyze_result_obj.get_result().crossover_result_tenkansen_kiusen) + " ," + str(ichi_analyze_result_obj.get_result().time_of_cross_tenkansen_kiusen) + " , " + str(tools.int_to_datetime(ichi_analyze_result_obj.get_result().time_of_cross_tenkansen_kiusen)))
    if ichi_analyze_result_obj.get_result().crossover_result_price_kiusen == ichi.ichi_crossover_price_kiusen_result_enum.Przeciecie_do_dolu :
        result_S.append(str(ichi_analyze_result_obj.get_result().crossover_result_price_kiusen) + " ," + str(ichi_analyze_result_obj.get_result().time_of_cross_price_kiusen) + " , " + str(tools.int_to_datetime(ichi_analyze_result_obj.get_result().time_of_cross_price_kiusen))) 
    if ichi_analyze_result_obj.get_result().crossover_price_senokuspan == ichi.ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_dolu :
        result_S.append(str(ichi_analyze_result_obj.get_result().crossover_price_senokuspan) + " ," + str(ichi_analyze_result_obj.get_result().time_of_cross_price_senokuspan) + " , " + str(tools.int_to_datetime(ichi_analyze_result_obj.get_result().time_of_cross_price_senokuspan)))
    if ichi_analyze_result_obj.get_result().crossover_price_senokuspan == ichi.ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_dolu:
        result_S.append(str(ichi_analyze_result_obj.get_result().crossover_price_senokuspan) + " ," + str(ichi_analyze_result_obj.get_result().time_of_cross_price_senokuspan) + " , " + str(tools.int_to_datetime(ichi_analyze_result_obj.get_result().time_of_cross_price_senokuspan)))

   

    return result_K , result_S


def analyze_adx_candles(candles:list[Candle] , product : ProductConf )  -> adx.adx_analyze_result_object:
    adx_obj = adx.adx_object()
    adx_data = adx_obj.get_data_from_candle_array(candles)
    

    adx_result , adx_trend ,  time_result = adx_obj.analyze_adx(adx_data , product.adx_adx, product.adx_window)
    if adx_trend != None:
        adx_analyze_result_obj = adx.adx_analyze_result_object(datetime.now(),product.symbol, cnf.PERIOD, adx_result, adx_trend)
        adx_analyze_result_obj.time =time_result
        print(f"Time adx result {tools.int_to_datetime( adx_analyze_result_obj.get_time())}")  

        if adx_result == adx.adx_result_enum.Wzrost_przeciecie or adx_result == adx.adx_result_enum.Spadek_przeciecie :
            exist = db.adx_result_exists_in_database(product.symbol , cnf.PERIOD, adx_analyze_result_obj , adx_trend)
            if  exist == False :
                db.save_adx_result_to_database(product.symbol , cnf.PERIOD, adx_analyze_result_obj , adx_trend)

        return adx_analyze_result_obj , adx_trend
    else:
        return None, None
def check_adx_to_close_transaction(candles:list[Candle] ,product:ProductConf, opened_transaction: tt.TransactionTrading) -> bool:
    adx_obj = adx.adx_object()
    adx_data = adx_obj.get_data_from_candle_array(candles)
    
    adx_result , adx_trend ,  time_result = adx_obj.analyze_adx(adx_data , product.adx_adx, product.adx_window)
    if adx_trend != None:
        if(opened_transaction.get_type() == "BUY" and adx_result == adx.adx_result_enum.Spadek_przeciecie) or (opened_transaction.get_type() == "SELL" and adx_result == adx.adx_result_enum.Wzrost_przeciecie):
            print(f"ADX Data godzina : {datetime.now() } , Symbol {product.symbol} , Result {adx_result} , Time {tools.int_to_datetime(time_result)}")
            return True
    return False

def analyze_mcad_candles(candles:list[Candle] , product : ProductConf) -> mcad.mcad_analyze_result_object: 
    mcd = mcad.mcad_object()
    mcad_data = mcd.get_data_from_candle_array(candles)
  
    mcad_result ,time_result = mcd.analyze_mcad(mcad_data , product.short_window_mcad , product.long_window_mcad , product.signal_window_mcad , product.angle_mcad)
    mcad_analyze_result_obj = mcad.mcad_analyze_result_object(datetime.now(),product.symbol, cnf.PERIOD, mcad_result )
    mcad_analyze_result_obj.time =time_result
    print(f"Time mcad result {tools.int_to_datetime(mcad_analyze_result_obj.get_time())}")
    
    if mcad_result == mcad.mcad_result_enum.Wzrost_przeciecie or mcad_result == mcad.mcad_result_enum.Spadek_przeciecie :
        exist = db.mcad_result_exists_in_database(product.symbol , cnf.PERIOD , mcad_analyze_result_obj)
        if  exist == False :
             db.save_mcad_result_to_database(product.symbol , cnf.PERIOD, mcad_analyze_result_obj)
       
    return mcad_analyze_result_obj

def check_mcad_to_close_transaction(candles:list[Candle] ,product:ProductConf, opened_transaction: tt.TransactionTrading) -> bool:
    mcd = mcad.mcad_object()
    mcad_data = mcd.get_data_from_candle_array(candles)
  
    mcad_result ,time_result = mcd.analyze_mcad(mcad_data , product.short_window_mcad , product.long_window_mcad , product.signal_window_mcad , product.angle_mcad)
    if mcad_result != None:
        if(opened_transaction.get_type() == "BUY" and mcad_result == mcad.mcad_result_enum.Spadek_przeciecie) or (opened_transaction.get_type() == "SELL" and mcad_result == mcad.mcad_result_enum.Wzrost_przeciecie):
            print(f"MCAD Data godzina : {datetime.now() } , Symbol {product.symbol} , Result {mcad_result} , Time {tools.int_to_datetime(time_result)}")
            return True
    return False

import asyncio
from datetime import datetime
import json
import pandas as pd


asyncio.run(main())

