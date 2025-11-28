import enum
import yfinance as yf
import pandas as pd
import math
from candle import Candle
import matplotlib.pyplot as plt
import tools

class Trend(enum.Enum):
    INCREASING = 1
    DECREASING = 2
    NEITHER = 3


class mcad_object :

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
    
    def _calculate_macd(self , stock_data, short_window=12, long_window=26, signal_window=9):
        stock_data['time']= stock_data['Date']
        stock_data['ShortEMA'] = stock_data['Close'].ewm(span=short_window, adjust=False).mean()
        stock_data['LongEMA'] = stock_data['Close'].ewm(span=long_window, adjust=False).mean()
        stock_data['MACD'] = stock_data['ShortEMA'] - stock_data['LongEMA']
        stock_data['Signal Line'] = stock_data['MACD'].ewm(span=signal_window, adjust=False).mean()
        stock_data['MACD_histogram'] = stock_data['MACD'] - stock_data['Signal Line']
        return stock_data

    def analyze_mcad(self ,stock_data_in, short_window=12, long_window=26, signal_window=9 , angle = 45):
    
    
        stock_data = self._calculate_macd(stock_data_in, short_window, long_window, signal_window)
        length = len(stock_data)

        list_of_data = []
        for i in range(length):
            list_of_data.append(stock_data.iloc[-i])

        list_of_data.reverse()

        list_of_data_time = []
        for data in list_of_data:
            list_of_data_time.append(data['time'])

        list_of_data_macd = []
        for data in list_of_data:
            list_of_data_macd.append(data['MACD'])

        list_of_data_signal = []
        for data in list_of_data:
            list_of_data_signal.append(data['Signal Line'])

        list_of_data_histogram = []
        for data in list_of_data:
            list_of_data_histogram.append(data['MACD_histogram'])


        crossover_point = self._calculate_crossover_point(list_of_data_macd, list_of_data_signal)
        time_of_cross = 0
        
        if crossover_point is not None:
            
            # sprawdzenie czy przeciecie jest wzrostowe czy spadkowe
            start_cross = crossover_point -4
            end_cross = crossover_point + 4
            
            if end_cross >= len(list_of_data_macd):
                 end_cross = len(list_of_data_macd) -1
           

            list_of_data_macd_cross = list_of_data_macd[start_cross:end_cross+1]
            list_of_data_signal_cross = list_of_data_signal[start_cross:end_cross+1]
            
            crossover_result = self._check_crossover(list_of_data_macd_cross, list_of_data_signal_cross)
            time_of_cross = int(list_of_data_time[crossover_point])
            t = tools.int_to_datetime(time_of_cross)
            
            print("Crossover result: "+ str(crossover_point) + " Time: " + t.strftime("%Y-%m-%d %H:%M:%S"))
            #sprawdzenie czy nie jest boczny
            start_direction = crossover_point
            end_direction = crossover_point +5
            if end_direction >= len(list_of_data_macd):
                 end_direction = len(list_of_data_macd) -1

           
            list_of_data_macd_direction = list_of_data_macd[start_direction:end_direction+1]
            list_of_data_signal_direction = list_of_data_signal[start_direction:end_direction+1]

            macd_trend = self._is_increasing_or_decreasing(list_of_data_macd_direction)
            signal_trend = self._is_increasing_or_decreasing(list_of_data_signal_direction)

            #sprawdzenie siły sygnalu
            start_histogram = start_direction
            end_histogram = end_direction

            if end_direction >= len(list_of_data_histogram):
                end_histogram = len(list_of_data_histogram) -1
            pointA = (start_histogram, abs(list_of_data_histogram[start_histogram]))
            pointB = (end_histogram,abs(list_of_data_histogram[end_histogram]))
            pointC = (end_histogram,abs(list_of_data_histogram[start_histogram]))

            lineA =  (pointA,pointB) #((start_histogram,list_of_data_histogram[start_histogram]),(end_histogram,list_of_data_histogram[end_histogram])) #DF
            lineB = (pointA , pointC) #((start_histogram,list_of_data_histogram[start_histogram]),(end_histogram,list_of_data_histogram[start_histogram])) #DE
            
            angle_of_crossing = self._angel(lineA, lineB)

            #if crossover_result == "MACD przecina do góry Signal Line" and macd_trend == Trend.INCREASING and signal_trend == Trend.INCREASING and angle_of_crossing > angle:
            #if crossover_result == "MACD przecina do góry Signal Line" and  angle_of_crossing > angle:
            if crossover_result == "MACD przecina do góry Signal Line":
                print(mcad_result_enum.Wzrost_przeciecie)
                return mcad_result_enum.Wzrost_przeciecie ,time_of_cross
            #if crossover_result == "MACD przecina do dołu Signal Line" and macd_trend == Trend.DECREASING and signal_trend == Trend.DECREASING and angle_of_crossing > angle:
            #if crossover_result == "MACD przecina do dołu Signal Line" and  angle_of_crossing > angle:
            if crossover_result == "MACD przecina do dołu Signal Line" :
                print(mcad_result_enum.Spadek_przeciecie)
                return mcad_result_enum.Spadek_przeciecie ,time_of_cross
            

        return mcad_result_enum.Boczny , time_of_cross

    def plot_mcad_linear_interpolation(self, stock_data_in , short_window=12, long_window=26, signal_window=9 , angle = 45):
        import matplotlib.pyplot as plt
        
        stock_data = self._calculate_macd(stock_data_in, short_window, long_window, signal_window)
        length = len(stock_data)

        list_of_data = []
        for i in range(length):
            list_of_data.append(stock_data.iloc[-i])

        list_of_data.reverse()

        list_of_data_time = []
        for data in list_of_data:
            list_of_data_time.append(data['time'])

        list_of_data_macd = []
        for data in list_of_data:
            list_of_data_macd.append(data['MACD'])

        list_of_data_signal = []
        for data in list_of_data:
            list_of_data_signal.append(data['Signal Line'])

        list_of_data_histogram = []
        for data in list_of_data:
            list_of_data_histogram.append(data['MACD_histogram'])


        crossover_point = self._calculate_crossover_point(list_of_data_macd, list_of_data_signal)

        if crossover_point is not None:
            
            # sprawdzenie czy przeciecie jest wzrostowe czy spadkowe
            start_cross = crossover_point -4
            end_cross = crossover_point + 4
            
            if end_cross >= len(list_of_data_macd):
                 end_cross = len(list_of_data_macd) -1
           

            list_of_data_macd_cross = list_of_data_macd[start_cross:end_cross+1]
            list_of_data_signal_cross = list_of_data_signal[start_cross:end_cross+1]
            
            print(list_of_data_macd_cross)
            print(list_of_data_signal_cross)

            crossover_result = self._check_crossover(list_of_data_macd_cross, list_of_data_signal_cross)
            time_of_cross = int(list_of_data_time[crossover_point])

            #sprawdzenie czy nie jest boczny
            start_direction = crossover_point
            end_direction = crossover_point +4
            if end_direction >= len(list_of_data_macd):
                 end_direction = len(list_of_data_macd) -1

           
            list_of_data_macd_direction = list_of_data_macd[start_direction:end_direction+1]
            list_of_data_signal_direction = list_of_data_signal[start_direction:end_direction+1]

            macd_trend = self._is_increasing_or_decreasing(list_of_data_macd_direction)
            signal_trend = self._is_increasing_or_decreasing(list_of_data_signal_direction)

            #sprawdzenie siły sygnalu
            start_histogram = start_direction
            end_histogram = end_direction

            if end_direction >= len(list_of_data_histogram):
                end_histogram = len(list_of_data_histogram) -1

            lineA = ((start_histogram,list_of_data_histogram[start_histogram]),(end_histogram,list_of_data_histogram[end_histogram])) #DF
            lineB = ((start_histogram,list_of_data_histogram[start_histogram]),(end_histogram,list_of_data_histogram[start_histogram])) #DE
            
            angle_of_crossing = self._angel(lineA, lineB)

            print(angle_of_crossing)
            print(crossover_result)
            print(macd_trend)
            print(signal_trend)
            
            if crossover_result == "MACD przecina do góry Signal Line" and macd_trend == Trend.INCREASING and signal_trend == Trend.INCREASING and angle_of_crossing > angle:
                print(mcad_result_enum.Wzrost_przeciecie)
              
            if crossover_result == "MACD przecina do dołu Signal Line" and macd_trend == Trend.DECREASING and signal_trend == Trend.DECREASING and angle_of_crossing > angle:
                print(mcad_result_enum.Spadek_przeciecie)
               
            

            plt.figure(figsize=(12, 8))
            ran_cross = range(start_cross, end_cross+1)
        
            ran_direction = range(start_direction, end_direction+1)
        
            ran_histogram = range(start_histogram, end_histogram+1)
            count = 100
            ran_mcad = range(0, len(list_of_data_macd))
            ran_signal = range(0, len(list_of_data_signal))
            ran_histogram_2 = range(crossover_point-count, len(list_of_data_histogram))

           # plt.plot([lineA[0][0], lineA[1][0]], [lineA[0][1], lineA[1][1]], label='MACD Histogram', color='purple')
           # plt.plot([lineB[0][0], lineB[1][0]], [lineB[0][1], lineB[1][1]], label='MACD Histogram', color='purple')
            plt.plot(ran_cross, list_of_data_macd_cross, label='MACD cross', color='green')
            plt.plot(ran_cross,list_of_data_signal_cross, label='Signal cross', color='orange')
            plt.plot(ran_direction, list_of_data_macd_direction, label='MACD direction', color='black')
            plt.plot(ran_direction, list_of_data_signal_direction, label='Signal direction', color='brown')
            plt.plot(ran_mcad, [list_of_data_macd[i] for i in ran_mcad], label='MACD', color='blue')
            plt.plot(ran_signal, [list_of_data_signal[i] for i in ran_signal] , label='Signal Line', color='red')
            plt.bar(ran_histogram_2, [list_of_data_histogram[i] for i in ran_histogram_2]  , label='MACD Histogram', color='purple')
            plt.legend(loc='upper left')
        

            plt.show(block=True)
            plt.pause(20)


   

    def _dot(self, vA, vB):
        return vA[0]*vB[0]+vA[1]*vB[1]

    def _angel(self,lineA, lineB):
        # Get nicer vector form
        vA = [(lineA[0][0]-lineA[1][0]), (lineA[0][1]-lineA[1][1])]
        vB = [(lineB[0][0]-lineB[1][0]), (lineB[0][1]-lineB[1][1])]
        # Get dot prod
        dot_prod = self._dot(vA, vB)
        # Get magnitudes
        magA = self._dot(vA, vA)**0.5
        magB = self._dot(vB, vB)**0.5
        # Get cosine value
        cos_ = dot_prod/magA/magB
        # Get angle in radians and then convert to degrees
        angle = math.acos(dot_prod/magB/magA)
        # Basically doing angle <- angle mod 360
        ang_deg = math.degrees(angle)%360
        
        if ang_deg-180>=0:
            # As in if statement
            return 360 - ang_deg
        else: 
            
            return ang_deg



    def _calculate_crossover_point(self , macd_line, signal_line):
            lst = []
            for i in range(len(macd_line) - 1, 0, -1):
                if (macd_line[i-1] < signal_line[i-1] and macd_line[i] > signal_line[i]) or (macd_line[i-1] > signal_line[i-1] and macd_line[i] < signal_line[i]):
                    lst.append(i)
                    lst.reverse()
                    return lst[0]

            
            return None

  
    
    def _check_crossover(self ,macd_line, signal_line):
            first_mcad = macd_line[0]
            first_signal = signal_line[0]
            last_mcad = macd_line[macd_line.__len__() - 1]
            last_signal = signal_line[signal_line.__len__() - 1]

            if pd.isna(last_mcad) or last_mcad == 0.0:
                last_mcad = macd_line[macd_line.__len__() - 2]

            if pd.isna(last_signal) or last_signal == 0.0:
                last_signal = signal_line[signal_line.__len__() - 2]
                
            if first_mcad < first_signal and last_mcad > last_signal:
                return "MACD przecina do góry Signal Line"
            elif first_mcad > first_signal and last_mcad < last_signal:
                return "MACD przecina do dołu Signal Line"
            
            return "Brak przecięcia"
            

            for i in range(1, len(macd_line)):
                if macd_line[i-1] < signal_line[i-1] and macd_line[i] > signal_line[i]:
                    return "MACD przecina do góry Signal Line"
                elif macd_line[i-1] > signal_line[i-1] and macd_line[i] < signal_line[i]:
                    return "MACD przecina do dołu Signal Line"
            return "Brak przecięcia"

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
           

   

class mcad_result_enum(enum.Enum) :
    Wzrost_przeciecie =1
    Spadek_przeciecie =2
    Boczny =3

class mcad_analyze_result_object :           
        def __init__(self ,time , symbol , period, result :mcad_result_enum) :
            self.result = result
            self.time = time
            self.symbol = symbol
            self.period = period
        def get_result(self):
            return self.result

        def get_time(self):
            return self.time

        def get_symbol(self):
            return self.symbol

        def get_period(self):
            return self.period