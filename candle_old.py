# Define the Candle class to represent a single candlestick in financial data
# class Candle:
#     # Initialize the Candle object with various attributes
#     def __init__(self, ctm: int, ctmString: str, open: float, high: float, low: float, close: float, vol: float):
#         self.ctm = ctm  # Timestamp of the candle
#         self.ctmString = ctmString  # String representation of the timestamp
#         self.open = open  # Opening price of the candle
#         self.high = high  # Highest price during the candle
#         self.low = low  # Lowest price during the candle
#         self.close = close  # Closing price of the candle
#         self.vol = vol  # Volume of the candle

#     # Define a string representation for the Candle object
#     def __repr__(self):
#         return f"Candle(ctm={self.ctm}, ctmString={self.ctmString}, open={self.open}, high={self.high}, low={self.low}, close={self.close}, vol={self.vol})"
    
#     # Class method to deserialize a list of candle data into Candle objects
#     @classmethod
#     def DeserialiazeCandels(cls, candles_data):
#         # Create a list of Candle objects from the input data
#         candles = [Candle(candle['ctm'], candle['ctmString'], candle['open'], candle['high'], candle['low'], candle['close'], candle['vol']) for candle in candles_data]
#         return candles
class Candle:
    def __init__(self, time: int,  open: float, high: float, low: float, close: float, tick_volume: float):

        self.time = time
      
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.tick_volume = tick_volume

   # def __repr__(self):
   #     return f"Candle(time={self.time},  open={self.open}, high={self.high}, low={self.low}, close={self.close}, vol={self.tick_volume})"
    
    @classmethod
    def DeserialiazeCandels(cls, candles_data):
       # print("DeserialiazeCandels : " + str(candles_data))
        candles = []
        import tools
        
        for candle in candles_data:
    
            cndl = candle
            t = cndl['time']
            if isinstance(t, (int, float)):
                timestamp = int(t)
                if timestamp <= 10_000_000_000:
                    timestamp = timestamp * 1000
            else:
                timestamp = tools.time_string_to_timestamp(t)
            tt = tools.int_to_datetime(timestamp)
            # Create a list of Candle objects from the input data
            candles.append(Candle(timestamp, candle['open'], candle['high'], candle['low'], candle['close'], candle['tick_volume']))
        return candles
    
    @classmethod
    def DeserialiazeCandel(cls, candle_data):
        
        t = candle_data['time']
        import tools
        if isinstance(t, (int, float)):
            timestamp = int(t)
            if timestamp <= 10_000_000_000:
                timestamp = timestamp * 1000
        else:
            timestamp = tools.time_string_to_timestamp(t)
        # Create a single Candle object from the input data
        return Candle(timestamp, candle_data['open'], candle_data['high'], candle_data['low'], candle_data['close'], candle_data['tick_volume'])