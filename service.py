from datetime import datetime, timedelta
import os
from symtable import Symbol
import time

from pathlib import Path
import sys

#import login as login
from config import conf as cnf
import logging as logger
from database import loadBaseData as load_base_data
import tools as tools
from api_broker import api_MT5 as api_mt5
from candle import Candle
from symbolx import SymbolX
from database import database as db
from wskazniki import mcad__chat as mcad

from wskazniki import adx__chat as adxcht
from database import loadBaseData  as loadBaseData
import asyncio
from datetime import datetime
from config.product_conf import ProductConf, ProductDB
import result as global_result
from wskazniki import ichi__chat as ichi
import pprint as pp
import transactiontraiding as tt
import status_communication as status_communication
import formacje_swiecowe as formacje_swiecowe
import zapis_rezultatu as zapis_rezultatu
import scenariusz as scenariusz
import zakoncz_scenariusz as zakoncz_scenariusz
from audit_log import log_trade_audit_event, _format_broker_time

load_current_data = False
debug_single_run =False # os.getenv("DEBUG_SINGLE_RUN", "0") == "1"
debug_session_attached = sys.gettrace() is not None
API_CLIENT = None

log_file_path = tools.logger_configuration()
DEFAULT_SL_DISTANCE = 40.0
DEFAULT_TP_DISTANCE = 80.0
DEFAULT_LOT_SIZE = 0.01
MT5_MAGIC_NUMBER = 234567


def _extract_latest_broker_time_ms(candles_data) -> int | None:
    if not candles_data:
        return None

    latest_row = candles_data[0]
    if latest_row is None:
        return None

    try:
        # DB row shape: time, open, high, low, close, volume, ...
        return int(latest_row[0])
    except Exception:
        return None


def _is_signal_within_max_time(candle_time: int, signal_time: int | None, max_time_result_minutes: int | None) -> bool:
    if signal_time is None:
        return False
    if max_time_result_minutes is None:
        return True

    diff_ms = int(candle_time) - int(signal_time)
    if diff_ms < 0:
        return False

    return diff_ms <= int(max_time_result_minutes) * 60_000


def _build_adx_for_scenario(
    adx_analyze_result_tuple,
    current_candle_time: int,
    symbol: str,
    period: str,
    runtime_state: dict,
):
    last_adx_buy_time = runtime_state.get("last_adx_buy_time")
    last_adx_sell_time = runtime_state.get("last_adx_sell_time")

    adx_filtered_signal = "BRAK"
    if adx_analyze_result_tuple is not None and adx_analyze_result_tuple[0] is not None:
        adx_result = adx_analyze_result_tuple[0].get_result()
        if adx_result == adxcht.adx_result_enum.Wzrost_przeciecie:
            adx_filtered_signal = "BUY"
        elif adx_result == adxcht.adx_result_enum.Spadek_przeciecie:
            adx_filtered_signal = "SELL"

    if adx_filtered_signal == "BUY":
        if adx_analyze_result_tuple[0] is not None and adx_analyze_result_tuple[0].get_time() is not None:
            last_adx_buy_time = int(adx_analyze_result_tuple[0].get_time())
        else:
            last_adx_buy_time = int(current_candle_time)
    elif adx_filtered_signal == "SELL":
        if adx_analyze_result_tuple[0] is not None and adx_analyze_result_tuple[0].get_time() is not None:
            last_adx_sell_time = int(adx_analyze_result_tuple[0].get_time())
        else:
            last_adx_sell_time = int(current_candle_time)

    runtime_state["last_adx_buy_time"] = last_adx_buy_time
    runtime_state["last_adx_sell_time"] = last_adx_sell_time

    adx_for_scenario = adx_analyze_result_tuple
    adx_trend_for_scenario = (
        adx_analyze_result_tuple[1]
        if adx_analyze_result_tuple is not None and adx_analyze_result_tuple[1] is not None
        else adxcht.Trend.NEITHER
    )

    buy_active = _is_signal_within_max_time(int(current_candle_time), last_adx_buy_time, cnf.MAX_TIME_RESULT)
    sell_active = _is_signal_within_max_time(int(current_candle_time), last_adx_sell_time, cnf.MAX_TIME_RESULT)

    if buy_active and (not sell_active or int(last_adx_buy_time) >= int(last_adx_sell_time)):
        adx_obj_for_scenario = adxcht.adx_analyze_result_object(
            time=int(last_adx_buy_time),
            symbol=symbol,
            period=period,
            result=adxcht.adx_result_enum.Wzrost_przeciecie,
            trend=adx_trend_for_scenario,
        )
        adx_for_scenario = (adx_obj_for_scenario, adx_trend_for_scenario)
    elif sell_active and (not buy_active or int(last_adx_sell_time) > int(last_adx_buy_time)):
        adx_obj_for_scenario = adxcht.adx_analyze_result_object(
            time=int(last_adx_sell_time),
            symbol=symbol,
            period=period,
            result=adxcht.adx_result_enum.Spadek_przeciecie,
            trend=adx_trend_for_scenario,
        )
        adx_for_scenario = (adx_obj_for_scenario, adx_trend_for_scenario)

    return adx_for_scenario


def _calculate_sl_tp(entry_price: float, side: str, sl_distance: float, tp_distance: float) -> tuple[float, float]:
    side_upper = str(side or "").upper()
    if side_upper == "BUY":
        return float(entry_price - sl_distance), float(entry_price + tp_distance)
    if side_upper == "SELL":
        return float(entry_price + sl_distance), float(entry_price - tp_distance)
    raise ValueError(f"Nieznany typ transakcji: {side}")


def _check_sl_tp_hit(side: str, candle_high: float, candle_low: float, stop_loss: float, take_profit: float) -> str | None:
    side_upper = str(side or "").upper()
    if side_upper == "BUY":
        if candle_low <= float(stop_loss):
            return "SL"
        if candle_high >= float(take_profit):
            return "TP"
        return None

    if side_upper == "SELL":
        if candle_high >= float(stop_loss):
            return "SL"
        if candle_low <= float(take_profit):
            return "TP"
        return None

    return None



# logger.basicConfig(filename=log_file_path, level=logger.INFO, 
#                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
async def main():
    global API_CLIENT

    try:
        API_CLIENT = api_mt5.API(
            login=cnf.USERNAME,
            password=cnf.PASSWORD,
            server=cnf.MT5_SERVER,
            path=cnf.MT5_PATH,
        )
    except Exception as mt5_error:
        if debug_single_run or debug_session_attached:
            API_CLIENT = None
            logger.warning(f"MT5 init failed in debug mode, continuing without broker API: {mt5_error}")
            print(f"MT5 init failed in debug mode, continuing without broker API: {mt5_error}")
        else:
            raise

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


      
 
    if load_current_data:
        result = loadBaseData.load_base_data(API_CLIENT)

        if result == False:
            print("Error loading base data")
            logger.error("Error loading base data")
            return
    products_list = ProductConf.load_products_from_json(os.path.dirname(os.path.abspath(__file__)) + '\\config\\products.json')
    opened_transactions_list = []
    num = 0
    symbol_runtime_state = {}
    while True:
        try:
            sleep_seconds = get_main_loop_sleep_seconds(cnf.PERIOD)
            num_candles = 1
            start_time = None
            end_time = None
            period= tools.Period.from_string((cnf.PERIOD)).value
            multiplication = tools.calculate_multiplication_v2(cnf.PERIOD)
            num = num+1
            run = True
            if run == True:
              for symbol in cnf.SYMBOLS_LIST:
                try:
                    print("--------------------------------------------------------------------------")
                    print("Processing symbol: " + symbol)
                    product = ProductConf.find_product_by_symbol(products_list, symbol)
                    if product is None:
                        print(f"Product not found for symbol {symbol}")
                        continue

                    if load_current_data:
                        start_time = tools.get_start_time(num_candles, period)*1000

                    saved = fetch_and_save_last_candle(symbol, period)
                    if not saved:
                        print(f"Failed to fetch and save candlestick data for {symbol}.")

                    data = db.get_last_candle_from_database(symbol, period, cnf.NUM_CANDLES) or []
                    broker_time_ms = _extract_latest_broker_time_ms(data)
                   
                    if len(data) == 0:
                        logger.warning(f"Brak świec w bazie dla {symbol} po próbie zapisu. Pomijam iterację symbolu.")
                        continue

                
                    if len(data) < cnf.NUM_CANDLES:
                        print(f"Expected {cnf.NUM_CANDLES} candels for symbol {symbol}, got {len(data)}")

                   
                    # DB zwraca rekordy od najnowszego, wskazniki liczymy rosnaco po czasie.
                    ordered_data = list(reversed(data))

                    #Formacje swiecowe
                    pattern_signal = formacje_swiecowe.analyze_open_signal(data)
                    if pattern_signal["signal"] is not None:

                       
                        print(
                            f"FORMACJE SWIECOWE: Symbol {symbol}, Signal {pattern_signal['signal']}, "
                            f"Patterns {pattern_signal['patterns']}, Time {tools.int_to_datetime(pattern_signal['candle_time'])}"
                        )

                    #ADX
                   
                    adx_analyze_result_obj = adxcht.analyze_adx_candles(
                        ordered_data,
                        product.adx_window,
                        product.adx_adx,
                        product.symbol,
                        cnf.PERIOD,
                    )
                    symbol_state = symbol_runtime_state.setdefault(symbol, {})
                    adx_for_scenario = _build_adx_for_scenario(
                        adx_analyze_result_obj,
                        current_candle_time=int(broker_time_ms) if broker_time_ms is not None else int(datetime.now().timestamp() * 1000),
                        symbol=product.symbol,
                        period=cnf.PERIOD,
                        runtime_state=symbol_state,
                    )
                    save_adx_result_to_database_if_needed(product, adx_analyze_result_obj)
                    if adx_analyze_result_obj[0] != None :
                        #print(f"Time adx result {tools.int_to_datetime( adx_analyze_result_obj[0].get_time())}")
                        if(adx_analyze_result_obj[0].get_result() != adxcht.adx_result_enum.Boczny) :
                            print(f"ADX Data godzina : {datetime.now() } , Symbol {symbol} , Result {adx_analyze_result_obj[0].get_result()} , Time {tools.int_to_datetime(adx_analyze_result_obj[0].get_time())}")
                           
                            zapis_rezultatu.log_symbol_result(
                                symbol=symbol,
                                adx_analyze_result_obj=adx_analyze_result_obj,
                                mcad_analyze_result_obj=None,
                                ichimoku_result_K=None,
                                ichimoku_result_S=None,
                                signal=None,
                            )
                       # else:
                        #    print(f"ADX Data godzina : {datetime.now() } , Symbol {symbol}")

                    #MCAD
                    mcad_analyze_result_obj = analyze_mcad_candles(ordered_data , product)
                    save_mcad_result_to_database_if_needed(product, mcad_analyze_result_obj)
                    if mcad_analyze_result_obj != None :
                        #print(f"Time mcad result {tools.int_to_datetime(mcad_analyze_result_obj.get_time())}")
                        if(mcad_analyze_result_obj.get_result() != mcad.mcad_result_enum.Boczny) :
                            print(f"MCAD Data godzina : {datetime.now() } , Symbol {symbol} , Result {mcad_analyze_result_obj.get_result()} , Time {tools.int_to_datetime(mcad_analyze_result_obj.get_time())}")
                            zapis_rezultatu.log_symbol_result(
                                symbol=symbol,
                                adx_analyze_result_obj=adx_analyze_result_obj,
                                mcad_analyze_result_obj=mcad_analyze_result_obj,
                                ichimoku_result_K=None,
                                ichimoku_result_S=None,
                                signal=None,
                                broker_time_ms=broker_time_ms,
                                event_type="INDICATOR_MCAD",
                            )
                        #else:
                         #   print(f"MCAD Data godzina : {datetime.now() } , Symbol {symbol}")

                    #ICHIMOKU
                    ichimoku_result_K , ichimoku_result_S = analzye_ichimoku_candles(ordered_data , product)
                    save_ichimoku_result_to_database_if_needed(product, ichimoku_result_K, ichimoku_result_S)

                    if ichimoku_result_K or ichimoku_result_S:
                        print(f"ICHIMOKU Data godzina : {datetime.now()} , Symbol {symbol} , Result_K {ichimoku_result_K} , Result_S {ichimoku_result_S}")
                        zapis_rezultatu.log_symbol_result(
                                symbol=symbol,
                                adx_analyze_result_obj=adx_analyze_result_obj,
                                mcad_analyze_result_obj=mcad_analyze_result_obj,
                                ichimoku_result_K=ichimoku_result_K,
                                ichimoku_result_S=ichimoku_result_S,
                                signal=None,
                            broker_time_ms=broker_time_ms,
                            event_type="INDICATOR_ICHIMOKU",
                            )
                    #else:
                  #      print(f"ICHIMOKU Data godzina : {datetime.now()} , Symbol {symbol}")
           
            

                    final_signal, scenario_number, scenario_conditions = analyze_scenarios(
                        symbol,
                        adx_for_scenario,
                        mcad_analyze_result_obj,
                        ichimoku_result_K,
                        ichimoku_result_S,
                        _extract_latest_broker_time_ms(data),
                        symbol_state,
                    )
                     
                            

                    if (
                        cnf.AUTO_OPEN_TRANSACTION
                        and final_signal == "BUY"
                        and status_communication.check_if_transaction_is_opened("BUY", symbol) == False
                    ):
                        status_communication.send_api_request_to_open_transaction("BUY", symbol)
                        log_trade_audit_event(
                            symbol=symbol,
                            event_type="OPEN_REQUEST_BUY",
                            signal="BUY",
                            broker_time_ms=broker_time_ms,
                            open_scenario_number=scenario_number,
                            scenario_conditions=scenario_conditions,
                        )
                        zapis_rezultatu.log_symbol_result(
                                symbol=symbol,
                                adx_analyze_result_obj=adx_analyze_result_obj,
                                mcad_analyze_result_obj=mcad_analyze_result_obj,
                                ichimoku_result_K=ichimoku_result_K,
                                ichimoku_result_S=ichimoku_result_S,
                                signal=None,
                            broker_time_ms=broker_time_ms,
                            event_type="OPEN_REQUEST_BUY",
                            )
                        if status_communication.check_api_signal_to_open_transaction("BUY", symbol):
                            status_communication.update_api_transaction_status("BUY", symbol, "OPENED")
                            log_trade_audit_event(
                                symbol=symbol,
                                event_type="OPENED_BUY",
                                signal="BUY",
                                broker_time_ms=broker_time_ms,
                                open_scenario_number=scenario_number,
                                scenario_conditions=scenario_conditions,
                            )
                            opened_transaction = tt.TransactionTrading(symbol, cnf.PERIOD, datetime.now(), "BUY", "OPEN")
                            if len(data) > 0:
                                entry_price = float(data[0][4])
                                stop_loss, take_profit = _calculate_sl_tp(
                                    entry_price=entry_price,
                                    side="BUY",
                                    sl_distance=DEFAULT_SL_DISTANCE,
                                    tp_distance=DEFAULT_TP_DISTANCE,
                                )
                                opened_transaction.entry_price = entry_price
                                opened_transaction.stop_loss = stop_loss
                                opened_transaction.take_profit = take_profit
                            already_opened = tools.transaction_already_opened(opened_transactions_list, opened_transaction)
                        
                            
                            if already_opened == False:
                                opened_transactions_list.append(opened_transaction)
                                db.save_transaction_to_database(symbol, cnf.PERIOD, datetime.now(), "BUY", "OPEN")
                                if API_CLIENT is not None:
                                    try:
                                        mt5_result = API_CLIENT.open_transaction(
                                            action="buy",
                                            _type=None,
                                            symbol=symbol,
                                            price=0,
                                            stop_loss=getattr(opened_transaction, "stop_loss", 0),
                                            take_profit=getattr(opened_transaction, "take_profit", 0),
                                            comment=f"SC{scenario_number}",
                                            lot_size=DEFAULT_LOT_SIZE,
                                            magic=MT5_MAGIC_NUMBER,
                                            ticket=None,
                                        )
                                        if mt5_result is not None and mt5_result.get("retcode") == 10009:
                                            opened_transaction.mt5_ticket = mt5_result.get("order")
                                            logger.info(f"MT5 BUY opened: symbol={symbol} ticket={opened_transaction.mt5_ticket}")
                                            print(f"MT5 BUY opened: symbol={symbol} ticket={opened_transaction.mt5_ticket}")
                                        else:
                                            logger.warning(f"MT5 BUY open failed: symbol={symbol} result={mt5_result}")
                                            print(f"MT5 BUY open failed: symbol={symbol} result={mt5_result}")
                                    except Exception as mt5_err:
                                        logger.error(f"MT5 BUY open exception: symbol={symbol} error={mt5_err}")
                                        print(f"MT5 BUY open exception: symbol={symbol} error={mt5_err}")
                                
                   
                        #else:
                        #    print("No time K result")
                        pass

                    # Check if there is already an opened transaction for this symbol
                    if (
                        cnf.AUTO_OPEN_TRANSACTION
                        and final_signal == "SELL"
                        and status_communication.check_if_transaction_is_opened("SELL", symbol) == False
                    ):
                        status_communication.send_api_request_to_open_transaction("SELL", symbol)
                        log_trade_audit_event(
                            symbol=symbol,
                            event_type="OPEN_REQUEST_SELL",
                            signal="SELL",
                            broker_time_ms=broker_time_ms,
                            open_scenario_number=scenario_number,
                            scenario_conditions=scenario_conditions,
                        )
                        zapis_rezultatu.log_symbol_result(
                                symbol=symbol,
                                adx_analyze_result_obj=adx_analyze_result_obj,
                                mcad_analyze_result_obj=mcad_analyze_result_obj,
                                ichimoku_result_K=ichimoku_result_K,
                                ichimoku_result_S=ichimoku_result_S,
                                signal=None,
                            broker_time_ms=broker_time_ms,
                            event_type="OPEN_REQUEST_SELL",
                            )
                        if status_communication.check_api_signal_to_open_transaction("SELL", symbol):
                            status_communication.update_api_transaction_status("SELL", symbol, "OPENED")
                            log_trade_audit_event(
                                symbol=symbol,
                                event_type="OPENED_SELL",
                                signal="SELL",
                                broker_time_ms=broker_time_ms,
                                open_scenario_number=scenario_number,
                                scenario_conditions=scenario_conditions,
                            )
                            opened_transaction = tt.TransactionTrading(symbol, cnf.PERIOD, datetime.now(), "SELL", "OPEN")
                            if len(data) > 0:
                                entry_price = float(data[0][4])
                                stop_loss, take_profit = _calculate_sl_tp(
                                    entry_price=entry_price,
                                    side="SELL",
                                    sl_distance=DEFAULT_SL_DISTANCE,
                                    tp_distance=DEFAULT_TP_DISTANCE,
                                )
                                opened_transaction.entry_price = entry_price
                                opened_transaction.stop_loss = stop_loss
                                opened_transaction.take_profit = take_profit
                            already_opened = tools.transaction_already_opened(opened_transactions_list, opened_transaction)
                        
                            if already_opened == False:
                                opened_transactions_list.append(opened_transaction)
                                db.save_transaction_to_database(symbol, cnf.PERIOD, datetime.now(), "SELL", "OPEN")
                                if API_CLIENT is not None:
                                    try:
                                        mt5_result = API_CLIENT.open_transaction(
                                            action="sell",
                                            _type=None,
                                            symbol=symbol,
                                            price=0,
                                            stop_loss=getattr(opened_transaction, "stop_loss", 0),
                                            take_profit=getattr(opened_transaction, "take_profit", 0),
                                            comment=f"SC{scenario_number}",
                                            lot_size=DEFAULT_LOT_SIZE,
                                            magic=MT5_MAGIC_NUMBER,
                                            ticket=None,
                                        )
                                        if mt5_result is not None and mt5_result.get("retcode") == 10009:
                                            opened_transaction.mt5_ticket = mt5_result.get("order")
                                            logger.info(f"MT5 SELL opened: symbol={symbol} ticket={opened_transaction.mt5_ticket}")
                                            print(f"MT5 SELL opened: symbol={symbol} ticket={opened_transaction.mt5_ticket}")
                                        else:
                                            logger.warning(f"MT5 SELL open failed: symbol={symbol} result={mt5_result}")
                                            print(f"MT5 SELL open failed: symbol={symbol} result={mt5_result}")
                                    except Exception as mt5_err:
                                        logger.error(f"MT5 SELL open exception: symbol={symbol} error={mt5_err}")
                                        print(f"MT5 SELL open exception: symbol={symbol} error={mt5_err}")
                       # else:
                        #    print("No time S result")
                        pass

                    zapis_rezultatu.log_symbol_result(
                        symbol=symbol,
                        adx_analyze_result_obj=adx_analyze_result_obj,
                        mcad_analyze_result_obj=mcad_analyze_result_obj,
                        ichimoku_result_K=ichimoku_result_K,
                        ichimoku_result_S=ichimoku_result_S,
                        signal=final_signal,
                        scenario_number=scenario_number,
                        scenario_conditions=scenario_conditions,
                        broker_time_ms=broker_time_ms,
                        event_type="SCENARIO_SIGNAL",
                    )

                    for opened_transaction in opened_transactions_list:
                        if opened_transaction.status != "OPEN":
                            continue
                        if opened_transaction.get_symbol() != symbol:
                            continue

                        close_type = opened_transaction.get_type()
                        close_result = {"close": False, "scenario_number": None, "scenario_conditions": "BRAK"}
                        close_event_type = "CLOSE_SIGNAL"

                        if len(data) > 0 and hasattr(opened_transaction, "stop_loss") and hasattr(opened_transaction, "take_profit"):
                            sl_tp_hit = _check_sl_tp_hit(
                                side=close_type,
                                candle_high=float(data[0][2]),
                                candle_low=float(data[0][3]),
                                stop_loss=float(opened_transaction.stop_loss),
                                take_profit=float(opened_transaction.take_profit),
                            )
                            if sl_tp_hit == "SL":
                                close_result = {
                                    "close": True,
                                    "scenario_number": 90,
                                    "scenario_conditions": (
                                        f"C90 CLOSE {close_type} BY STOP LOSS | "
                                        f"ENTRY={opened_transaction.entry_price}; SL={opened_transaction.stop_loss}; TP={opened_transaction.take_profit}"
                                    ),
                                }
                                close_event_type = "CLOSE_SL"
                            elif sl_tp_hit == "TP":
                                close_result = {
                                    "close": True,
                                    "scenario_number": 91,
                                    "scenario_conditions": (
                                        f"C91 CLOSE {close_type} BY TAKE PROFIT | "
                                        f"ENTRY={opened_transaction.entry_price}; SL={opened_transaction.stop_loss}; TP={opened_transaction.take_profit}"
                                    ),
                                }
                                close_event_type = "CLOSE_TP"

                        if close_result.get("close") is not True:
                            close_result = zakoncz_scenariusz.get_close_signal(
                                opened_transaction_type=close_type,
                                adx_analyze_result_obj=adx_for_scenario,
                                mcad_analyze_result_obj=mcad_analyze_result_obj,
                                ichimoku_result_k=ichimoku_result_K,
                                ichimoku_result_s=ichimoku_result_S,
                                period=cnf.PERIOD,
                                max_time_result_minutes=cnf.MAX_TIME_RESULT,
                            )

                        if close_result.get("close") is not True:
                            continue

                        close_scenario = close_result.get("scenario_number")
                        broker_time_ms = _extract_latest_broker_time_ms(data)
                        print(
                            f"ZAMKNIECIE SCENARIUSZ | symbol={opened_transaction.get_symbol()} | "
                            f"data_godzina={datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
                            f"typ={close_type} | numer_scenariusza=C{close_scenario} | "
                            f"godzina_brokera={_format_broker_time(broker_time_ms)}"
                        )
                        close_message = (
                            f"ZAMKNIECIE SCENARIUSZ | symbol={opened_transaction.get_symbol()} | "
                            f"data_godzina={datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
                            f"typ={close_type} | numer_scenariusza=C{close_scenario} | "
                            f"godzina_brokera={_format_broker_time(broker_time_ms)}"
                        )
                        log_close_scenario_hit_to_file(opened_transaction.get_symbol(), close_message)
                        zapis_rezultatu.log_symbol_result(
                            symbol=opened_transaction.get_symbol(),
                            adx_analyze_result_obj=adx_analyze_result_obj,
                            mcad_analyze_result_obj=mcad_analyze_result_obj,
                            ichimoku_result_K=ichimoku_result_K,
                            ichimoku_result_S=ichimoku_result_S,
                            signal=f"CLOSE_{close_type}",
                            close_scenario_number=close_scenario,
                            scenario_conditions=close_result.get("scenario_conditions"),
                            broker_time_ms=broker_time_ms,
                            event_type=close_event_type,
                        )
                        log_trade_audit_event(
                            symbol=opened_transaction.get_symbol(),
                            event_type=close_event_type,
                            signal=close_type,
                            broker_time_ms=broker_time_ms,
                            close_scenario_number=close_scenario,
                            scenario_conditions=close_result.get("scenario_conditions"),
                        )

                        status_communication.send_signal_to_close_transaction(close_type, opened_transaction.get_symbol())
                        log_trade_audit_event(
                            symbol=opened_transaction.get_symbol(),
                            event_type=f"{close_event_type}_REQUEST",
                            signal=close_type,
                            broker_time_ms=broker_time_ms,
                            close_scenario_number=close_scenario,
                            scenario_conditions=close_result.get("scenario_conditions"),
                        )
                        if status_communication.check_get_signal_to_close_transaction(close_type, opened_transaction.get_symbol()):
                            mt5_ticket = getattr(opened_transaction, "mt5_ticket", None)
                            if API_CLIENT is not None and mt5_ticket is not None:
                                try:
                                    mt5_close_result = API_CLIENT.close_transaction(
                                        ticket=int(mt5_ticket),
                                        comment=f"CLOSE_SC{close_scenario}",
                                    )
                                    if mt5_close_result is not None and mt5_close_result.get("retcode") == 10009:
                                        logger.info(f"MT5 position closed: symbol={opened_transaction.get_symbol()} ticket={mt5_ticket}")
                                        print(f"MT5 position closed: symbol={opened_transaction.get_symbol()} ticket={mt5_ticket}")
                                    else:
                                        logger.warning(f"MT5 close failed: symbol={opened_transaction.get_symbol()} ticket={mt5_ticket} result={mt5_close_result}")
                                        print(f"MT5 close failed: symbol={opened_transaction.get_symbol()} ticket={mt5_ticket} result={mt5_close_result}")
                                except Exception as mt5_close_err:
                                    logger.error(f"MT5 close exception: symbol={opened_transaction.get_symbol()} ticket={mt5_ticket} error={mt5_close_err}")
                                    print(f"MT5 close exception: symbol={opened_transaction.get_symbol()} ticket={mt5_ticket} error={mt5_close_err}")
                            opened_transaction.set_status("CLOSE")
                            log_trade_audit_event(
                                symbol=opened_transaction.get_symbol(),
                                event_type=f"{close_event_type}_CONFIRMED",
                                signal=close_type,
                                broker_time_ms=broker_time_ms,
                                close_scenario_number=close_scenario,
                                scenario_conditions=close_result.get("scenario_conditions"),
                            )
                            db.update_transaction_status(
                                opened_transaction.get_symbol(),
                                cnf.PERIOD,
                                opened_transaction.get_time(),
                                close_type,
                                "CLOSE",
                            )
               
                except Exception as symbol_error:
                   
                    logger.error(f"Błąd przetwarzania symbolu {symbol}: {symbol_error}")
                    print(f"Błąd przetwarzania symbolu {symbol}: {symbol_error}")
                    continue

            if debug_single_run:
                print("DEBUG_SINGLE_RUN=1 -> kończę po jednej iteracji pętli.")
                break

            print(f"p {sleep_seconds}s before next iteration for period {cnf.PERIOD}")
            wait_loop(sleep_seconds)
        except Exception as loop_error:
            logger.error(f"Błąd głównej pętli while: {loop_error}")
            print(f"Błąd głównej pętli while: {loop_error}")
            wait_loop(5)


def analyze_scenarios(
    symbol: str,
    adx_analyze_result_obj,
    mcad_analyze_result_obj,
    ichimoku_result_K: list[str],
    ichimoku_result_S: list[str],
    broker_time_ms: int | None,
    runtime_state: dict,
) -> tuple[str, int | None, str | None]:
    final_signal = "BRAK"
    scenario_number = None
    scenario_conditions = "BRAK"

    scenario_result = scenariusz.get_trade_signal(
        adx_analyze_result_obj,
        mcad_analyze_result_obj,
        ichimoku_result_K,
        ichimoku_result_S,
        cnf.PERIOD,
        cnf.MAX_TIME_RESULT,
    )
    scenario_number = scenario_result.get("scenario_number")
    scenario_conditions = scenario_result.get("scenario_conditions") or "BRAK"

    scenario_signal = scenario_result.get("signal")
    if scenario_signal not in ("BUY", "SELL"):
        return final_signal, None, "BRAK"

    signal_key = (
        str(scenario_signal),
        int(scenario_number) if scenario_number is not None else None,
        int(broker_time_ms) if broker_time_ms is not None else None,
    )
    last_signal_key = runtime_state.get("last_open_signal_key")
    if signal_key == last_signal_key:
        # Nie emituj OPEN_SIGNAL wielokrotnie dla tej samej swiecy/scenariusza.
        return "BRAK", None, "BRAK"

    runtime_state["last_open_signal_key"] = signal_key

    if scenario_signal == "BUY":
        final_signal = "BUY"
        scenario_message = (
            f"SCENARIUSZ HIT | symbol={symbol} | data_godzina={datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
            f"| sygnal=BUY | numer_scenariusza=SC{scenario_number} | "
            f"godzina_brokera={_format_broker_time(broker_time_ms)}"
        )
        print(scenario_message)
        log_scenario_hit_to_file(symbol, scenario_message)
        log_trade_audit_event(
            symbol=symbol,
            event_type="OPEN_SIGNAL",
            signal="BUY",
            broker_time_ms=broker_time_ms,
            open_scenario_number=scenario_number,
            scenario_conditions=scenario_conditions,
        )
        return final_signal, scenario_number, scenario_conditions

    if scenario_signal == "SELL":
        final_signal = "SELL"
        scenario_message = (
            f"SCENARIUSZ HIT | symbol={symbol} | data_godzina={datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
            f"| sygnal=SELL | numer_scenariusza=SC{scenario_number} | "
            f"godzina_brokera={_format_broker_time(broker_time_ms)}"
        )
        print(scenario_message)
        log_scenario_hit_to_file(symbol, scenario_message)
        log_trade_audit_event(
            symbol=symbol,
            event_type="OPEN_SIGNAL",
            signal="SELL",
            broker_time_ms=broker_time_ms,
            open_scenario_number=scenario_number,
            scenario_conditions=scenario_conditions,
        )
        return final_signal, scenario_number, scenario_conditions

    return "BRAK", None, "BRAK"


def log_scenario_hit_to_file(symbol: str, message: str) -> None:
    try:
        logs_dir = Path(__file__).resolve().parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        file_path = logs_dir / f"scenariusz_{symbol}.txt"
        with file_path.open("a", encoding="utf-8") as file:
            file.write(message + "\n")
    except Exception as log_error:
        logger.error(f"Nie udalo sie zapisac logu scenariusza dla {symbol}: {log_error}")


def log_close_scenario_hit_to_file(symbol: str, message: str) -> None:
    try:
        logs_dir = Path(__file__).resolve().parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        file_path = logs_dir / f"zakoncz_scenariusz_{symbol}.txt"
        with file_path.open("a", encoding="utf-8") as file:
            file.write(message + "\n")
    except Exception as log_error:
        logger.error(f"Nie udalo sie zapisac logu scenariusza zamkniecia dla {symbol}: {log_error}")

def analzye_ichimoku_candles(candles:list[Candle] , product : ProductConf) -> tuple[list[str], list[str]]:
    if candles is None or len(candles) == 0:
        return [], []

    ichi_obj = ichi.ichimoku_object()
    ichi_data = ichi_obj.get_data_from_candle_array(candles)
    if ichi_data.empty:
        return [], []

    last_n = min(30, len(ichi_data))
    last_n_candles_df = ichi_data.tail(last_n).copy()

    ichi_analyze_result_obj, _ = ichi.analyze_ichimoku_candles(
        data=ichi_data,
        last_n_candles=last_n_candles_df,
        tenkansen_period=product.tenkansen_period,
        kiusen_period=product.kijunsen_period,
        senokuspanB_period=product.senkouspan_period,
        symbol=product.symbol,
        period=cnf.PERIOD,
    )
    if ichi_analyze_result_obj is None:
        return [], []

    ichi_result_data = ichi_analyze_result_obj.get_result()
    result_K = []
    result_S = []

    def append_result(target: list[str], result_name: str, time_value):
        if time_value is None:
            return
        time_int = int(time_value)
        target.append(result_name + " ," + str(time_int) + " , " + str(tools.int_to_datetime(time_int)))

    if ichi_result_data.crossover_result_tenkansen_kiusen == ichi.ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_gory:
        append_result(result_K, str(ichi_result_data.crossover_result_tenkansen_kiusen), ichi_result_data.time_of_cross_tenkansen_kiusen)
    if ichi_result_data.crossover_result_price_kiusen == ichi.ichi_crossover_price_kiusen_result_enum.Przeciecie_do_gory:
        append_result(result_K, str(ichi_result_data.crossover_result_price_kiusen), ichi_result_data.time_of_cross_price_kiusen)

    senkou_result = ichi_result_data.crossover_price_senokuspan
    if senkou_result == ichi.ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_gory:
        append_result(result_K, "ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_gory", ichi_result_data.time_of_cross_price_senokuspan)

    if ichi_result_data.crossover_result_tenkansen_kiusen == ichi.ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_dolu:
        append_result(result_S, str(ichi_result_data.crossover_result_tenkansen_kiusen), ichi_result_data.time_of_cross_tenkansen_kiusen)
    if ichi_result_data.crossover_result_price_kiusen == ichi.ichi_crossover_price_kiusen_result_enum.Przeciecie_do_dolu:
        append_result(result_S, str(ichi_result_data.crossover_result_price_kiusen), ichi_result_data.time_of_cross_price_kiusen)
    if senkou_result == ichi.ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_dolu:
        append_result(result_S, "ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_dolu", ichi_result_data.time_of_cross_price_senokuspan)

   

    return result_K , result_S


def save_ichimoku_result_to_database_if_needed(
    product: ProductConf,
    ichimoku_result_K: list[str] | None,
    ichimoku_result_S: list[str] | None,
) -> None:
    def _save_single(raw_result: str) -> None:
        parts = tools.split_string_by_comma(raw_result)
        if len(parts) < 2:
            return

        result_name = str(parts[0]).strip()
        result_time_raw = str(parts[1]).strip()
        if result_time_raw == "":
            return

        try:
            result_time = int(result_time_raw)
        except Exception:
            return

        exist = db.ichimoku_result_exists_in_database(product.symbol, cnf.PERIOD, result_time, result_name)
        if exist is False:
            db.save_ichimoku_result_to_database(product.symbol, cnf.PERIOD, result_time, result_name)

    for result in (ichimoku_result_K or []):
        _save_single(result)

    for result in (ichimoku_result_S or []):
        _save_single(result)


def save_adx_result_to_database_if_needed(
    product: ProductConf,
    adx_analyze_result_tuple: tuple[adxcht.adx_analyze_result_object | None, adxcht.Trend | None],
) -> None:
    adx_result_obj, adx_trend = adx_analyze_result_tuple

    if adx_result_obj is None or adx_trend is None:
        return

    adx_result = adx_result_obj.get_result()
    if adx_result not in (adxcht.adx_result_enum.Wzrost_przeciecie, adxcht.adx_result_enum.Spadek_przeciecie):
        return

    exist = db.adx_result_exists_in_database(product.symbol, cnf.PERIOD, adx_result_obj, adx_trend)
    if exist is False:
        db.save_adx_result_to_database(product.symbol, cnf.PERIOD, adx_result_obj, adx_trend)

def check_adx_to_close_transaction(candles:list[Candle] ,product:ProductConf, opened_transaction: tt.TransactionTrading) -> bool:
    if candles is None or len(candles) < max(3, int(product.adx_window or 14)):
        return False

    adx_obj = adxcht.adx_object()
    adx_data = adx_obj.get_data_from_candle_array(candles)
    adx_df = adx_obj.calculate_adx(adx_data, period=product.adx_window)

    if adx_df.empty or len(adx_df) < 3:
        return False

    if adx_obj.buy_signal(adx_df, adx_threshold=product.adx_adx):
        adx_result = adxcht.adx_result_enum.Wzrost_przeciecie
    elif adx_obj.sell_signal(adx_df, adx_threshold=product.adx_adx):
        adx_result = adxcht.adx_result_enum.Spadek_przeciecie
    else:
        adx_result = adxcht.adx_result_enum.Boczny

    transaction_type = (opened_transaction.get_type() or "").upper()
    should_close = (transaction_type == "BUY" and adx_result == adxcht.adx_result_enum.Spadek_przeciecie) or (
        transaction_type == "SELL" and adx_result == adxcht.adx_result_enum.Wzrost_przeciecie
    )

    if should_close:
        time_result = int(adx_data.iloc[-1]["Date"])
        print(f"ADX Data godzina : {datetime.now() } , Symbol {product.symbol} , Result {adx_result} , Time {tools.int_to_datetime(time_result)}")
        return True

    return False

def analyze_mcad_candles(candles:list[Candle] , product : ProductConf) -> mcad.mcad_analyze_result_object | None: 
    mcd = mcad.mcad_object()
    mcad_data = mcd.get_data_from_candle_array(candles)
  
    mcad_result, time_result, trend, raw_cross, raw_cross_time = mcd.analyze_mcad(
        mcad_data,
        product.short_window_mcad,
        product.long_window_mcad,
        product.signal_window_mcad,
        product.angle_mcad,
    )
    if mcad_result is None or time_result is None:
        return None

    mcad_analyze_result_obj = mcad.mcad_analyze_result_object(
        time=time_result,
        symbol=product.symbol,
        period=cnf.PERIOD,
        result=mcad_result,
        trend=trend,
    )
    mcad_analyze_result_obj.set_raw_cross(raw_cross, raw_cross_time)
    print(f"Time mcad result {tools.int_to_datetime(mcad_analyze_result_obj.get_time())}")

    return mcad_analyze_result_obj


def save_mcad_result_to_database_if_needed(
    product: ProductConf,
    mcad_analyze_result_obj: mcad.mcad_analyze_result_object | None,
) -> None:
    if mcad_analyze_result_obj is None:
        return

    mcad_result = mcad_analyze_result_obj.get_result()
    if mcad_result not in (mcad.mcad_result_enum.Wzrost_przeciecie, mcad.mcad_result_enum.Spadek_przeciecie):
        return

    exist = db.mcad_result_exists_in_database(product.symbol, cnf.PERIOD, mcad_analyze_result_obj)
    if exist is False:
        db.save_mcad_result_to_database(product.symbol, cnf.PERIOD, mcad_analyze_result_obj)

def check_mcad_to_close_transaction(candles:list[Candle] ,product:ProductConf, opened_transaction: tt.TransactionTrading) -> bool:
    mcd = mcad.mcad_object()
    mcad_data = mcd.get_data_from_candle_array(candles)
  
    mcad_result, time_result, _trend, _raw_cross, _raw_cross_time = mcd.analyze_mcad(
        mcad_data,
        product.short_window_mcad,
        product.long_window_mcad,
        product.signal_window_mcad,
        product.angle_mcad,
    )
    if mcad_result is not None and time_result is not None:
        if(opened_transaction.get_type() == "BUY" and mcad_result == mcad.mcad_result_enum.Spadek_przeciecie) or (opened_transaction.get_type() == "SELL" and mcad_result == mcad.mcad_result_enum.Wzrost_przeciecie):
            print(f"MCAD Data godzina : {datetime.now() } , Symbol {product.symbol} , Result {mcad_result} , Time {tools.int_to_datetime(time_result)}")
            return True
    return False

def fetch_and_save_last_candle(symbol: str, period: int, max_retries: int = 3, retry_delay: int = 2) -> bool:
    def _period_seconds(period_value: int) -> int:
        period_int = int(period_value)
        if period_int <= 0:
            raise ValueError(f"Niepoprawny period: {period_value}")
        return period_int * 60

    def _to_epoch_ms(value) -> int:
        if isinstance(value, (int, float)):
            numeric = int(value)
            return numeric if numeric > 10_000_000_000 else numeric * 1000

        text = str(value).strip()
        if text.isdigit():
            numeric = int(text)
            return numeric if numeric > 10_000_000_000 else numeric * 1000

        return int(tools.time_string_to_timestamp(text))

    for attempt in range(1, max_retries + 1):
        try:
            if API_CLIENT is None:
                raise RuntimeError("API client is not initialized")

            cycle_seconds = _period_seconds(period)
            
            latest_candle = API_CLIENT.get_last_candle(symbol, period)
          
            if not latest_candle:
                logger.warning(f"Brak danych świecy z get_last_candle dla {symbol}, próba {attempt}/{max_retries}")
                time.sleep(retry_delay)
                continue

            latest_time_ms = _to_epoch_ms(latest_candle["time"])
            latest_time_ts = int(latest_time_ms // 1000)
            cycle_start_ts = (latest_time_ts // cycle_seconds) * cycle_seconds
            cycle_start_ms = cycle_start_ts * 1000

            open_value = float(latest_candle["open"])
            close_value = float(latest_candle["close"])
            high_value = float(latest_candle["high"])
            low_value = float(latest_candle["low"])
            tick_value = float(latest_candle.get("tick_volume", 0.0))

            last_rows = db.get_last_candle_from_database(symbol, period, 1) or []
            if last_rows:
                last_row = last_rows[0]
                last_cycle_ms = _to_epoch_ms(last_row[0])
                if last_cycle_ms == cycle_start_ms:
                    logger.info(f"Candle already exists for {symbol} period {period} time {cycle_start_ms}. Skip save.")
                    return True

            aggregated_candle = {
                "time": cycle_start_ms,
                "open": open_value,
                "high": high_value,
                "low": low_value,
                "close": close_value,
                "tick_volume": tick_value,
            }

            db.save_candle_to_database(symbol, period, aggregated_candle)
            return True
        except Exception as e:
            print(f"Error while fetching/saving candle for {symbol}: {e}")
            logger.error(f"Error while fetching/saving candle for {symbol} on attempt {attempt}: {e}")
            time.sleep(retry_delay)

    logger.error(f"Nie udało się zapisać świecy dla {symbol} po {max_retries} próbach")
    return False

def get_main_loop_sleep_seconds(period_code: str) -> int:
    period = (period_code or "").upper()
    period_seconds_map = {
        "M1": 60,
        "M5": 5 * 60,
        "M15": 15 * 60,
        "M30": 30 * 60,
        "H1": 60 * 60,
        "H4": 4 * 60 * 60,
        "D1": 24 * 60 * 60,
    }

    full_period_seconds = period_seconds_map.get(period)
    if full_period_seconds is None:
        return 40

    return max(1, int(full_period_seconds / 2))


def wait_loop(seconds: int) -> None:
    end_time = time.time() + max(0, int(seconds))
    while True:
        remaining = end_time - time.time()
        if remaining <= 0:
            break
        time.sleep(min(1, remaining))


import asyncio
from datetime import datetime
import json


asyncio.run(main())

