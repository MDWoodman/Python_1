import os
import datetime
import logging

from candle import Candle


def logger_configuration():
    def get_current_file_path():
        return os.path.abspath(__file__)

    def get_log_file_path():
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        base_path = os.path.dirname(get_current_file_path())
        log_dir = os.path.join(base_path, 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        return os.path.join(log_dir, f'login_{current_date}.log')

# Configure logging with the current date in the filename
    log_file_path = get_log_file_path()

# Create handlers
    file_handler = logging.FileHandler(log_file_path)
    console_handler = logging.StreamHandler()

# Set the level and format for the handlers
    file_handler.setLevel(logging.INFO)
    console_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

# Get the root logger
    root_logger = logging.getLogger()

# Add the handlers to the root logger
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

# Set the level for the root logger  
    root_logger.setLevel(logging.DEBUG)

    return log_file_path

def calculate_multiplication(period):
    if period == 'M1':
        return 60
    elif period == 'M5':
        return 300
    elif period == 'M15':
        return 900
    elif period == 'M30':
        return 1800
    elif period == 'H1':
        return 3600
    elif period == 'H4':
        return 14400
    elif period == 'D1':
        return 86400
    elif period == 'W1':
        return 604800
    elif period == 'MN1':
        return 2592000
    else:
        raise ValueError(f"Unsupported period: {period}")   

def calculate_multiplication_v2(period):
    if period == 'M1':
        return 1
    elif period == 'M5':
        return 2
    elif period == 'M15':
        return 15
    elif period == 'M30':
        return 30
    elif period == 'H1':
        return 60
    elif period == 'H4':
        return 240
    elif period == 'D1':
        return 86400
    elif period == 'W1':
        return 604800
    elif period == 'MN1':
        return 2592000
        
    else:
        raise ValueError(f"Unsupported period: {period}")   
    
def FormatCandles(candle : Candle) :

    new_open = candle.open
    new_close =candle.open + candle.close
    new_high = candle.open + candle.high
    new_low = candle.open + candle.low
    new_vol = candle.tick_volume
    new_ctmString = candle.time

    new_candle = Candle( new_ctmString, new_open, new_high, new_low, new_close, new_vol)
   
    return new_candle

def get_end_time():
    return int(datetime.datetime.now().timestamp() + 3600)
def get_start_time(num_candles, period):

    kor = 0
    if period == Period.M1.value:
        start_time = int((datetime.datetime.now() - datetime.timedelta(minutes=num_candles)).timestamp())
        kor=30
    elif period == Period.M5.value:
        start_time = int((datetime.datetime.now() - datetime.timedelta(minutes=5 * num_candles)).timestamp())
        kor=120
    elif period == Period.M15.value:
        start_time = int((datetime.datetime.now() - datetime.timedelta(minutes=15 * num_candles)).timestamp())
        kor=300
    elif period == Period.M30.value:
        start_time = int((datetime.datetime.now() - datetime.timedelta(minutes=30 * num_candles)).timestamp())
        kor=600
    elif period == Period.H1.value:
        start_time = int((datetime.datetime.now() - datetime.timedelta(hours=num_candles)).timestamp())
    elif period == Period.H4.value:
        start_time = int((datetime.datetime.now() - datetime.timedelta(hours=4 * num_candles)).timestamp())
    else:
        raise ValueError("Unsupported period value")
    
    
    start_time = start_time + 3600+kor
    return start_time
from enum import Enum

class Period(Enum):
    M1 = 1        # 1 minute
    def from_string(period_str):
        try:
            return Period[period_str]
        except KeyError:
            raise ValueError(f"Unsupported period: {period_str}")
    M5 = 5        # 5 minutes
    M15 = 15      # 15 minutes
    M30 = 30      # 30 minutes
    H1 = 60       # 60 minutes (1 hour)
    H4 = 240      # 240 minutes (4 hours)
    D1 = 1440     # 1440 minutes (1 day)
    W1 = 10080    # 10080 minutes (1 week)
    MN1 = 43200   # 43200 minutes (30 days)


def draw_chart(candles_data):
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    dates = [datetime.fromtimestamp(candle['time']) for candle in candles_data]
    opens = [candle['open'] for candle in candles_data]
    highs = [candle['high'] for candle in candles_data]
    lows = [candle['low'] for candle in candles_data]
    closes = [candle['close'] for candle in candles_data]

    fig, ax = plt.subplots()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
    plt.xticks(rotation=45)

    for i in range(len(dates)):
        color = 'green' if closes[i] >= opens[i] else 'red'
        ax.plot([dates[i], dates[i]], [lows[i], highs[i]], color='black')
        ax.plot([dates[i], dates[i]], [opens[i], closes[i]], color=color, linewidth=5)

    plt.xlabel('Time')
    plt.ylabel('Price')
    plt.title('Candlestick Chart')
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    return ax

def update_chart(ax, new_candles_data):
    import matplotlib.dates as mdates

    dates = [datetime.datetime.fromtimestamp(candle['time']) for candle in new_candles_data]
    opens = [candle['open'] for candle in new_candles_data]
    highs = [candle['high'] for candle in new_candles_data]
    lows = [candle['low'] for candle in new_candles_data]
    closes = [candle['close'] for candle in new_candles_data]

    for i in range(len(dates)):
        color = 'green' if closes[i] >= opens[i] else 'red'
        ax.plot([dates[i], dates[i]], [lows[i], highs[i]], color='black')
        ax.plot([dates[i], dates[i]], [opens[i], closes[i]], color=color, linewidth=5)

    ax.figure.canvas.draw()
def position_size(account_risk_usd, stop_loss_pips, pair, price,volume=1):
    """
    account_risk_usd: kwota, którą chcesz ryzykować (np. 100 USD)
    stop_loss_pips: odległość SL w pipsach (np. 50)
    pair: np. 'EURUSD', 'USDJPY'
    price: bieżący kurs instrumentu
    """
    pip_val_per_lot = pip_value(0.1, pair, price)
    lot_size = account_risk_usd / (stop_loss_pips * pip_val_per_lot)
    return lot_size
def pip_value(lot_size, pair, price):
    """
    lot_size: liczba lotów (np. 1, 0.1, 0.01)
    pair: np. 'EURUSD', 'USDJPY'
    price: bieżący kurs instrumentu
    """
    if pair.endswith('JPY'):
        pip = 0.01
    else:
        pip = 0.0001

    contract_size = 100_000  # standardowy lot
    
    value = pip * contract_size * lot_size

    if pair.startswith('USD'):
        # USD jako waluta bazowa → wartość w USD
        return value / price
    else:
        return value
def int_to_datetime(timestamp):
    if isinstance(timestamp, str):
        timestamp = int(timestamp)
    return datetime.datetime.fromtimestamp(timestamp/1000)
def get_max_time_from_list(int_list : []) -> int:
    """
    Returns the maximum integer value from a list of integers.

    Args:
        int_list (list): A list of integers.

    Returns:
        int: The maximum integer value from the list.
    """
    if not int_list:
        return 0
    return max(int_list)

import transactiontraiding as tt
def  transaction_already_opened(opened_transactions_list : list[tt.TransactionTrading], opened_transaction: tt.TransactionTrading) -> bool:         
   return any(
       t.symbol == opened_transaction.symbol and t.status == "OPEN"
       for t in opened_transactions_list
   )

def time_string_to_timestamp(time_string: str) -> int:
    """
    Converts a time string in the format 'YYYY.MM.DD HH:MM' to a Unix timestamp.

    Args:
        time_string (str): The time string to convert (e.g., '2025.04.28 12:00').

    Returns:
        int: The corresponding Unix timestamp.
    """
    from datetime import datetime

    # Parse the time string into a datetime object
    dt_object = datetime.strptime(time_string, '%Y.%m.%d %H:%M')

    # Convert the datetime object to a Unix timestamp and return it as an integer
    return int(dt_object.timestamp()*1000)

def split_string_by_comma(input_string: str) -> list:
    """
    Splits a string by commas, removes whitespace from each substring, and returns a list of the resulting substrings.

    Args:
        input_string (str): The string to split.

    Returns:
        list: A list of substrings split by commas with whitespace removed.
    """
    return [substring.strip() for substring in input_string.split(',')]

   

