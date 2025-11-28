import mcad as mcd
import database as db
import tools as tools
import adx as adx
from candle import Candle


data = db.get_all_candles_from_database('USDJPY', '5')
#data = db.get_all_candles_from_database('GBPUSD', '5')

if data:
     candles = []
     for d in data:
         candle = Candle(d[0], d[1], d[2], d[3], d[4], d[5], d[6])
         candles.append(candle)
     print(candles)
     adx = adx.ADX_object()
     candls_data = adx.get_data_from_candle_array(candles)
     print(candls_data)
     #stock_data = mcd.calculate_macd(candls_data)
     #print(stock_data)
    # plus_di, minus_di, adx_smooth = adx.get_adx(candls_data['High'], candls_data['Low'], candls_data['Close'], 14)
    
     adx_result , time =  adx.analyze_adx(candls_data,20,14)
    
     time_nrm = tools.int_to_datetime(time)
     print(time_nrm)
     print(adx_result)
     #mcad.plot_macd(candls_data)
    # mcad.plot_mcad_linear_interpolation(candls_data)
   #  result = mcad.check_macd_crossovers(candls_data,20)
 #    print(result)
     print("Done")
    