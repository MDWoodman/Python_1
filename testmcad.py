import mcad as mcd
import database as db
import tools as tools

from candle import Candle


data = db.get_all_candles_from_database('USDJPY', '5')
#data = db.get_all_candles_from_database('GBPUSD', '5')

if data:
     candles = []
     for d in data:
         candle = Candle(d[0], d[1], d[2], d[3], d[4], d[5], d[6])
         candles.append(candle)
     print(candles)
     mcd = mcd.mcad_object()
     candls_data = mcd.get_data_from_candle_array(candles)
     #stock_data = mcd.calculate_macd(candls_data)
     #print(stock_data)
     mcd.plot_mcad_linear_interpolation(candls_data)
     #mcad.plot_macd(candls_data)
    # mcad.plot_mcad_linear_interpolation(candls_data)
   #  result = mcad.check_macd_crossovers(candls_data,20)
 #    print(result)
     print("Done")
    