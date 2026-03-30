
import pandas as pd
import math
from candle import Candle
from enum import Enum
import matplotlib.pyplot as plt
import tools as tools

class adx_object:
    def get_data_from_candle_array(self , candle_array: list[Candle]):

        data = {
            'Date': [candle.time for candle in candle_array],
            'Open': [candle.open for candle in candle_array],
            'High': [candle.high for candle in candle_array],
            'Low': [candle.low for candle in candle_array],
            'Close': [candle.close for candle in candle_array],
            'Volume': [candle.tick_volume for candle in candle_array]
        }
        stock_data = pd.DataFrame(data)
#  stock_data.set_index('Date', inplace=True)
        return stock_data
    def get_adx(self, high, low, close,time_in, lookback):
        up_move = high.diff()
        down_move = -low.diff()

        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
        time = time_in

        tr1 = pd.DataFrame(high - low)
        tr2 = pd.DataFrame(abs(high - close.shift(1)))
        tr3 = pd.DataFrame(abs(low - close.shift(1)))
        frames = [tr1, tr2, tr3]
        tr = pd.concat(frames, axis = 1, join = 'inner').max(axis = 1)
        atr = tr.rolling(lookback).mean()
        
        plus_di = 100 * (plus_dm.ewm(alpha = 1/lookback).mean() / atr)
        minus_di = 100 * (minus_dm.ewm(alpha = 1/lookback).mean() / atr)
        dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
        adx = ((dx.shift(1) * (lookback - 1)) + dx) / lookback
        adx_smooth = adx.ewm(alpha = 1/lookback).mean()
        return plus_di, minus_di, adx_smooth,time
    
    def analyze_adx(self ,stock_data_in, adx_conf , lookback=14):

       

        high = stock_data_in['High']
        low = stock_data_in['Low']
        close = stock_data_in['Close']

        plus_di, minus_di, adx ,time = self.get_adx(high, low, close,time_in ,lookback)
    
        data = pd.DataFrame(
            {
                "plus_di": plus_di,
                "minus_di": minus_di,
                "adx": adx,
                "time": time,
            }
        ).dropna()

        if data.empty or len(data) < 3:
            return adx_result_enum.Boczny, None, None

        list_of_data_plus_di = data["plus_di"].tolist()
        list_of_data_minus_di = data["minus_di"].tolist()
        list_of_data_adx = data["adx"].tolist()
        list_of_data_time = data["time"].tolist()

        crossover_point = self._calculate_crossover_point(list_of_data_plus_di, list_of_data_minus_di)

        if crossover_point is not None:
            if crossover_point <= 0:
                return adx_result_enum.Boczny, None, None

            prev_plus = list_of_data_plus_di[crossover_point - 1]
            prev_minus = list_of_data_minus_di[crossover_point - 1]
            curr_plus = list_of_data_plus_di[crossover_point]
            curr_minus = list_of_data_minus_di[crossover_point]

            crossover_result = "Brak przecięcia"
            if prev_plus < prev_minus and curr_plus > curr_minus:
                crossover_result = "D+ przecina do góry D-"
            elif prev_plus > prev_minus and curr_plus < curr_minus:
                crossover_result = "D+ przecina do dołu D-"

            time_of_cross = int(list_of_data_time[crossover_point])
            print(tools.int_to_datetime(time_of_cross))
            adx_value = list_of_data_adx[crossover_point]


            start_adx = crossover_point
            end_adx = crossover_point +4
            if end_adx >= len(list_of_data_adx):
                 end_adx= len(list_of_data_adx) -1

            
            list_of_data_adx_trend = list_of_data_adx[start_adx:end_adx+1]
            adx_trend = self._is_increasing_or_decreasing(list_of_data_adx_trend)

            if crossover_result == "D+ przecina do góry D-" and adx_value < adx_conf : #and adx_trend == Trend.INCREASING:
                print(adx_result_enum.Wzrost_przeciecie)
                return adx_result_enum.Wzrost_przeciecie , adx_trend , time_of_cross
            if crossover_result == "D+ przecina do dołu D-" and adx_value < adx_conf : # and adx_trend == Trend.INCREASING:
                print(adx_result_enum.Spadek_przeciecie)
                return adx_result_enum.Spadek_przeciecie , adx_trend , time_of_cross

            return adx_result_enum.Boczny , adx_trend , time_of_cross
        else:
            return adx_result_enum.Boczny , None , None
    
    def _is_increasing_or_decreasing(self ,data):
            first = data[0]
            if pd.isna(first):
                first = data[1]

            last = data[data.__len__() - 1]
            if pd.isna(last):
                 last = data[data.__len__() - 2]

            if first < last:
                return Trend.INCREASING
            elif first > last:
                return Trend.DECREASING
            else:
                return Trend.NEITHER  
    def _calculate_crossover_point(self , plus_di, minus_di):
            lst=[]
            for i in range(len(plus_di) - 1, 0, -1):
                if (plus_di[i-1] < minus_di[i-1] and plus_di[i] > minus_di[i]) or (plus_di[i-1] > minus_di[i-1] and plus_di[i] < minus_di[i]):
                    lst.append(i)
                    lst.reverse()
                    return lst[0]
            return None
    def _check_crossover(self ,plus_di_line, minus_di_line):
            
            first_plus_di=plus_di_line[0]
            if pd.isna(first_plus_di):
                first_plus_di = plus_di_line[1]
            
            first_minus_di = minus_di_line[0]
            if pd.isna(first_minus_di):
                first_minus_di = minus_di_line[1]
            last_plus_di = plus_di_line[plus_di_line.__len__() - 1]
            if pd.isna(last_plus_di):
                last_plus_di = plus_di_line[plus_di_line.__len__() - 2]
            last_minus_di = minus_di_line[minus_di_line.__len__() - 1]
            if pd.isna(last_minus_di):
                last_minus_di = minus_di_line[minus_di_line.__len__() - 2]

           
            if first_plus_di < first_minus_di and last_plus_di > last_minus_di:
                return "D+ przecina do góry D-"
            elif first_plus_di > first_minus_di and last_plus_di < last_minus_di:
                return "D+ przecina do dołu D-"
            
            return "Brak przecięcia"

    def _check_crossover_v2(self ,plus_di_line, minus_di_line):
            
            for i in range(len(plus_di_line) - 1, 0, -1):
                if (plus_di_line[i-1] < minus_di_line[i-1] and plus_di_line[i] > minus_di_line[i]) or (plus_di_line[i-1] > minus_di_line[i-1] and plus_di_line[i] < minus_di_line[i]):
                    first_plus_di=plus_di_line[i-1]
                    first_minus_di = minus_di_line[i-1]
                    last_plus_di = plus_di_line[i]
                    last_minus_di = minus_di_line[i]
                    if first_plus_di < first_minus_di and last_plus_di > last_minus_di:
                        return "D+ przecina do góry D-"
                    elif first_plus_di > first_minus_di and last_plus_di < last_minus_di:
                        return "D+ przecina do dołu D-"
                    
            return "Brak przecięcia"
          
              

class adx_result_enum(Enum):
    Wzrost_przeciecie = 1
    Spadek_przeciecie = 2
    Boczny = 3

class Trend(Enum):
    INCREASING = 1
    DECREASING = 2
    NEITHER = 3

class adx_analyze_result_object:    
    def __init__(self, time, symbol, period, result: adx_result_enum , trend: Trend):
        self.result = result
        self.time = time
        self.symbol = symbol
        self.period = period
        self.trend = trend

    def get_time(self):
        return self.time

    def get_symbol(self):
        return self.symbol

    def get_period(self):
        return self.period

    def get_result(self):
        return self.result
    
    def get_trend(self):
        return self.trend
#aapl['plus_di'] = pd.DataFrame(get_adx(aapl['high'], aapl['low'], aapl['close'], 14)[0]).rename(columns = {0:'plus_di'})
#aapl['minus_di'] = pd.DataFrame(get_adx(aapl['high'], aapl['low'], aapl['close'], 14)[1]).rename(columns = {0:'minus_di'})
#aapl['adx'] = pd.DataFrame(get_adx(aapl['high'], aapl['low'], aapl['close'], 14)[2]).rename(columns = {0:'adx'})
#aapl = aapl.dropna()
#aapl.tail()