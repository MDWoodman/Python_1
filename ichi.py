from enum import Enum
import pandas as pd

from adx import Trend
from candle import Candle



class ichimoku_object:
   
    def get_data_from_candle_array(self, candle_array : list[Candle]):
        data = {
            'Date': [candle.time for candle in candle_array],
            'Open': [candle.open for candle in candle_array],
            'High': [candle.high for candle in candle_array],
            'Low': [candle.low for candle in candle_array],
            'Close': [candle.close for candle in candle_array],
            'Volume': [candle.tick_volume for candle in candle_array]
        }
        stock_data = pd.DataFrame(data)
        return stock_data

    def calculate_ichimoku(self, stock_data , tenkansen, kiusen, senokuspan):
        data = stock_data.copy()
        # Calculate Tenkan-sen (Conversion Line)
        data['Tenkan_sen'] = (data['High'].rolling(window=tenkansen).max() + data['Low'].rolling(window=tenkansen).min()) / 2

        # Calculate Kijun-sen (Base Line)
        data['Kijun_sen'] = (data['High'].rolling(window=kiusen).max() + data['Low'].rolling(window=kiusen).min()) / 2

        # Calculate Senkou Span A (Leading Span A)
        data['Senkou_Span_A'] = ((data['Tenkan_sen'] + data['Kijun_sen']) / 2).shift(kiusen)

        # Calculate Senkou Span B (Leading Span B)
        data['Senkou_Span_B'] = ((data['High'].rolling(window=senokuspan).max() + data['Low'].rolling(window=senokuspan).min()) / 2).shift(kiusen)

        # Calculate Chikou Span (Lagging Span)
        data['Chikou_Span'] = data['Close'].shift(-kiusen)
        
        #data["Date"] = pd.to_datetime(data["Date"])
        return data
    def plot_ichimoku(self, stock_data):
        import matplotlib.pyplot as plt

        plt.figure(figsize=(14, 7))
        plt.plot(stock_data['Date'], stock_data['Close'], label='Close Price', color='black')
        plt.plot(stock_data['Date'], stock_data['Tenkan_sen'], label='Tenkan-sen', color='red')
        plt.plot(stock_data['Date'], stock_data['Kijun_sen'], label='Kijun-sen', color='blue')
        plt.plot(stock_data['Date'], stock_data['Senkou_Span_A'], label='Senkou Span A', color='green')
        plt.plot(stock_data['Date'], stock_data['Senkou_Span_B'], label='Senkou Span B', color='brown')
        plt.plot(stock_data['Date'], stock_data['Chikou_Span'], label='Chikou Span', color='purple')

        plt.fill_between(stock_data['Date'], stock_data['Senkou_Span_A'], stock_data['Senkou_Span_B'], where=stock_data['Senkou_Span_A'] >= stock_data['Senkou_Span_B'], facecolor='lightgreen', interpolate=True)
        plt.fill_between(stock_data['Date'], stock_data['Senkou_Span_A'], stock_data['Senkou_Span_B'], where=stock_data['Senkou_Span_A'] < stock_data['Senkou_Span_B'], facecolor='lightcoral', interpolate=True)

        plt.title('Ichimoku Kinko Hyo')
        plt.xlabel('Date')
        plt.ylabel('Price')
        plt.legend()
        plt.grid()
        plt.show()

    def analyze_ichimoku(self, stock_data_in , last_n_candles: Candle,  tenkansen_period , kiusen_period , senokuspanB_period):
         
        stoc_data = self.calculate_ichimoku(stock_data_in , tenkansen_period , kiusen_period , senokuspanB_period)

        #self.plot_ichimoku(stoc_data)

        list_of_data_tenkan_sen = []
        for i in range(len(stoc_data['Tenkan_sen'])):
            list_of_data_tenkan_sen.append(stoc_data['Tenkan_sen'].iloc[-i])

        list_of_data_tenkan_sen.reverse()

        list_of_data_kiusen = []
        for i in range(len(stoc_data['Kijun_sen'])):
            list_of_data_kiusen.append(stoc_data['Kijun_sen'].iloc[-i])

        list_of_data_kiusen.reverse()

        list_of_data_senkou_span_a = []

        for i in range(len(stoc_data['Senkou_Span_A'])):
            list_of_data_senkou_span_a.append(stoc_data['Senkou_Span_A'].iloc[-i])

        list_of_data_senkou_span_a.reverse()

        list_of_data_senkou_span_b = []

        for i in range(len(stoc_data['Senkou_Span_B'])):
            list_of_data_senkou_span_b.append(stoc_data['Senkou_Span_B'].iloc[-i])

        list_of_data_senkou_span_b.reverse()

        list_of_data_chikou_span = []

        for i in range(len(stoc_data['Chikou_Span'])):
            list_of_data_chikou_span.append(stoc_data['Chikou_Span'].iloc[-i])

        list_of_data_chikou_span.reverse()

        list_of_data_time = []

        for i in range(len(stoc_data['Date'])):
            list_of_data_time.append(stoc_data['Date'].iloc[-i])

        list_of_data_time.reverse()

        ichi_result_obj = ichi_result_object()

        crossover_point = self._calculate_crossover_point(list_of_data_tenkan_sen, list_of_data_kiusen)
        #przeciecie tenkanes i kiusen
        if crossover_point is not None:

            start_cross = crossover_point
            end_cross = crossover_point
            start_cross = crossover_point - 7
            end_cross = crossover_point + 4
            if end_cross >= len(list_of_data_tenkan_sen):
                end_cross = len(list_of_data_tenkan_sen) - 1

            list_of_data_tenkan_sen_cross = list_of_data_tenkan_sen[start_cross:end_cross + 1]
            list_of_data_kiusen_cross = list_of_data_kiusen[start_cross:end_cross + 1]

            crossover_result = self._check_crossover(list_of_data_tenkan_sen_cross, list_of_data_kiusen_cross)
            time_of_cross = int(list_of_data_time[crossover_point])
           
            ichi_result_obj.crossover_result_tenkansen_kiusen = crossover_result
            ichi_result_obj.time_of_cross_tenkansen_kiusen = time_of_cross

        crossover_price_kiusen_result , time_of_cross_price_kiusen = self._check_crossover_price_kiusen(last_n_candles, list_of_data_kiusen)
       

        ichi_result_obj.crossover_result_price_kiusen = crossover_price_kiusen_result 
        ichi_result_obj.time_of_cross_price_kiusen = time_of_cross_price_kiusen

        crossover_price_senokuspan , time_of_cross_price_senokuspan =   self._check_crossover_price_senokuspan_a_senokuspan_b(last_n_candles, list_of_data_senkou_span_a, list_of_data_senkou_span_b , list_of_data_time)
        

        ichi_result_obj.crossover_price_senokuspan = crossover_price_senokuspan
        ichi_result_obj.time_of_cross_price_senokuspan = time_of_cross_price_senokuspan

        return ichi_result_obj
        
    def _check_crossover_price_senokuspan_a_senokuspan_b(self, last_n_candles, senkou_span_a, senkou_span_b , list_of_data_time):

        import tools
        result = []
        
        for i in range(len(last_n_candles) - 1, 0, -1):
            idx=0
            for j in range(len(list_of_data_time) - 1, 0, -1):
                if last_n_candles[i].time == list_of_data_time[j]:
                    idx = j
                    break
            if pd.isna(senkou_span_a[idx]) == False:
                if last_n_candles[i].open > senkou_span_a[idx] and last_n_candles[i].close < senkou_span_a[idx]:
                    result.append(str(i) + ", Cena przecina do dołu SenkouSpanA" +"," + str(last_n_candles[i].time) + ", " + str(tools.int_to_datetime(last_n_candles[i].time)))
                elif last_n_candles[i].open < senkou_span_a[idx] and last_n_candles[i].close > senkou_span_a[idx]:
                    result.append( str(i) + ", Cena przecina do góry SenkouSpanA" + "," + str(last_n_candles[i].time) + ", " + str(tools.int_to_datetime(last_n_candles[i].time)))
            
               
        for i in range(len(last_n_candles) - 1, 0, -1):
            idx=0
            for j in range(len(senkou_span_b) - 1, 0, -1):
                if last_n_candles[i].time == list_of_data_time[j]:
                    idx = j
                    break
            if pd.isna(senkou_span_b[idx]) == False:
                if last_n_candles[i].open > senkou_span_b[idx] and last_n_candles[i].close < senkou_span_b[idx]:
                    result.append(str(i) + ", Cena przecina do dołu SenkouSpanB"  +","+ str(last_n_candles[i].time) + ", " + str(tools.int_to_datetime(last_n_candles[i].time)))
                elif last_n_candles[i].open < senkou_span_b[idx] and last_n_candles[i].close > senkou_span_b[idx]:
                    result.append( str(i) + ", Cena przecina do góry SenkouSpanB" + ","+ str(last_n_candles[i].time )+ ", " + str(tools.int_to_datetime(last_n_candles[i].time)))
    

        if(result.__len__() > 0):
            result_sorted = self.sort_by_first_number(result)
            result_sorted.reverse()
            return str(result_sorted[0].split(',')[1]).strip(), str(result_sorted[0].split(',')[2]).strip()
        
        return "Brak przecięcia", last_n_candles[0].time

    def sort_by_first_number(self, items):
        def extract_first_number(item):
            # Split the string by spaces and take the first part, then convert to integer
            return int(item.split()[0].replace(",",""))
        
        # Sort the items using the extracted number as the key
        sorted_items = sorted(items, key=extract_first_number)
        return sorted_items   
    
    def _check_crossover_price_kiusen(self, last_n_candles, kiusen):
          
        kiu = kiusen[kiusen.__len__() - 1]
        if pd.isna(kiu):
            kiu = kiusen[kiusen.__len__() - 2]

        for j in range(len(kiusen) - 1, 0, -1):
            for i in range(len(last_n_candles) - 1, 0, -1):
                kiu = kiusen[j]
                if(pd.isna(kiu) == False):
                    if last_n_candles[i].open > kiu and last_n_candles[i].close < kiu:
                        return ichi_crossover_price_kiusen_result_enum.Przeciecie_do_dolu, last_n_candles[i].time
                    elif last_n_candles[i].open < kiu and last_n_candles[i].close > kiu:
                        return ichi_crossover_price_kiusen_result_enum.Przeciecie_do_gory, last_n_candles[i].time
            
 
        return ichi_crossover_price_kiusen_result_enum.Brak_przeciecia , last_n_candles[0].time

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
    def _calculate_crossover_point(self , tenkan_sen, kiusen):
            for i in range(len(tenkan_sen) - 1, 0, -1):
                if (tenkan_sen[i-1] < kiusen[i-1] and tenkan_sen[i] > kiusen[i]) or (tenkan_sen[i-1] > kiusen[i-1] and tenkan_sen[i] < kiusen[i]):
                    return i
            return None
    def _check_crossover(self ,tenkan_sen, kiusen):
            
            first_tenkan_sen = tenkan_sen[0]
            if pd.isna(first_tenkan_sen):
                first_tenkan_sen = tenkan_sen[1]
            
            first_kiusen = kiusen[0]
            if pd.isna(first_kiusen):
                first_kiusen = kiusen[1]
            last_tenkan_sen = tenkan_sen[tenkan_sen.__len__() - 1]
            if pd.isna(last_tenkan_sen):
                last_tenkan_sen = tenkan_sen[tenkan_sen.__len__() - 2]
            last_kiusen = kiusen[kiusen.__len__() - 1]
            if pd.isna(last_kiusen):
                last_kiusen = kiusen[kiusen.__len__() - 2]

           
            if first_tenkan_sen < first_kiusen and last_tenkan_sen > last_kiusen:
                return ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_gory
            elif first_tenkan_sen > first_kiusen and last_tenkan_sen < last_kiusen:
                return ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_dolu
            
            return ichi_crossover_tenkansen_kiusen_result_enum.Brak_przeciecia
            

    


class ichi_crossover_result_enum(Enum):
    Wzrost_przeciecie = 1
    Spadek_przeciecie = 2
    Boczny = 3

class ichi_crossover_price_kiusen_result_enum(Enum):
    Przeciecie_do_gory = 1
    Przeciecie_do_dolu = 2
    Brak_przeciecia = 3

class ichi_crossover_price_senokuspan_result_enum(Enum):
    Przeciecie_do_gory = 1
    Przeciecie_do_dolu = 2
    Brak_przeciecia = 3

class ichi_crossover_tenkansen_kiusen_result_enum(Enum):
    Przeciecie_do_gory = 1
    Przeciecie_do_dolu = 2
    Brak_przeciecia = 3


class ichi_result_object:
    def __init__(self):
        self._crossover_result_tenkansen_kiusen = None
        self._time_of_cross_tenkansen_kiusen = None

        self._crossover_result_price_kiusen = None
        self._time_of_cross_price_kiusen = None

        self._crossover_price_senokuspan = None
        self._time_of_cross_price_senokuspan = None

    @property
    def crossover_result_price_kiusen(self):
        return self._crossover_result_price_kiusen

    @crossover_result_price_kiusen.setter
    def crossover_result_price_kiusen(self, value):
        self._crossover_result_price_kiusen = value

    @property
    def time_of_cross_price_kiusen(self):
        return self._time_of_cross_price_kiusen

    @time_of_cross_price_kiusen.setter
    def time_of_cross_price_kiusen(self, value):
        self._time_of_cross_price_kiusen = value

    @property
    def crossover_price_senokuspan(self):
        return self._crossover_price_senokuspan

    @crossover_price_senokuspan.setter
    def crossover_price_senokuspan(self, value):
        self._crossover_price_senokuspan = value

    @property
    def time_of_cross_price_senokuspan(self):
        return self._time_of_cross_price_senokuspan

    @time_of_cross_price_senokuspan.setter
    def time_of_cross_price_senokuspan(self, value):
        self._time_of_cross_price_senokuspan = value

    @property
    def crossover_result_tenkansen_kiusen(self):
        return self._crossover_result_tenkansen_kiusen

    @crossover_result_tenkansen_kiusen.setter
    def crossover_result_tenkansen_kiusen(self, value):
        self._crossover_result_tenkansen_kiusen = value

    @property
    def time_of_cross_tenkansen_kiusen(self):
        return self._time_of_cross_tenkansen_kiusen

    @time_of_cross_tenkansen_kiusen.setter
    def time_of_cross_tenkansen_kiusen(self, value):
        self._time_of_cross_tenkansen_kiusen = value

class ichimoku_analyze_result_object :
    def __init__(self, time, symbol, period, result : ichi_result_object):

        self._symbol = symbol
        self._period = period
        self._result = result
        self._time = time

    @property
    def symbol(self):
        return self._symbol

    @symbol.setter
    def symbol(self, value):
        self._symbol = value

    @property
    def period(self):
        return self._period

    @period.setter
    def period(self, value):
        self._period = value

    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, value):
        self._result = value

    def get_time(self):
        return self._time

    def get_result(self):
        return self._result

    def get_symbol(self):
        return self._symbol

    def get_period(self):
        return self._period

    def __str__(self):
        return f"Symbol {self._symbol} , Result {self._result} , Time {self._time} , Period {self._period}"