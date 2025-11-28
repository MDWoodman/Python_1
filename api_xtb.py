from APIConnector_MT4 import *


class API :
  
    #client = APIClient()
    def __init__(self, user_id: str, password: str) -> None:
        
        self.user_id = user_id
        self.password = password
        self.client = APIClient()
        self.client.timeout = 10

    def login_xstation(self):
     
        loginResponse = self.client.execute(loginCommand(userId=self.user_id, password=self.password))
        logger.info(str(loginResponse)) 

        # check if user logged in correctly
        if(loginResponse['status'] == False):
            print('Login failed. Error code: {0}'.format(loginResponse['errorCode']))
            return

        # get ssId from login response
        ssid = loginResponse['streamSessionId']
        return loginResponse

    def logout(self) -> bool:
        # Simulate a successful logout
        return True

    def get_current_price(self , product: str):
        """
        Fetches the current price (bid and ask) for a given product using xStation 5 API.

        Args:
            product (str): The symbol of the product (e.g., 'EURUSD', 'WTI').
        
        Returns:
            dict: A dictionary with bid, ask, and spread values, or None if an error occurs.
        """
 
        try:
            # Log in to the API
           
            response = self.client.commandExecute("getTickPrices", {
                "symbols": [product],
                "timestamp": 0  # Use 0 to get the latest prices
            })

            if response['status']:
                # Extract the first result for the requested product
                price_data = response['returnData'][0]
                return {
                    "symbol": price_data["symbol"],
                    "bid": price_data["bid"],
                    "ask": price_data["ask"],
                    "spread": round(price_data["ask"] - price_data["bid"], 5)
                }
            else:
                raise Exception(f"Failed to fetch price data for {product}.")

        except Exception as e:
            print(f"Error: {e}")
            return None
    def get_current_price(self, product: str):
        """
        Fetches the current price (bid and ask) for a given product using xStation 5 API.

        Args:
            product (str): The symbol of the product (e.g., 'EURUSD', 'WTI').

        Returns:
            dict: A dictionary with bid, ask, and spread values, or None if an error occurs.
        """
        try:
            # Log in to the API
            response = self.client.commandExecute("getTickPrices", {
                "symbols": [product],
                "timestamp": 0  # Use 0 to get the latest prices
            })

            if response['status']:
                # Extract the first result for the requested product
                price_data = response['returnData'][0]
                return {
                    "symbol": price_data["symbol"],
                    "bid": price_data["bid"],
                    "ask": price_data["ask"],
                    "spread": price_data["ask"] - price_data["bid"]
                }
        except Exception as e:
            logger.error(f"Error fetching current price for {product}: {e}")
            return None

    def open_trade(self, symbol: str, volume: float, order_type: str, sl: float = None, tp: float = None):
        """
        Opens a trading position using xStation 5 API.

        Args:
            symbol (str): The symbol of the product (e.g., 'EURUSD', 'WTI').
            volume (float): The volume of the trade.
            order_type (str): The type of order ('buy' or 'sell').
            sl (float, optional): Stop loss value.
            tp (float, optional): Take profit value.

        Returns:
            dict: A dictionary with the trade result, or None if an error occurs.
        """
        try:
            trade_command = {
                "symbol": symbol,
                "volume": volume,
                "type": order_type,
                "customComment": "Trade opened via API"
            }

            if sl is not None:
                trade_command["sl"] = sl

            if tp is not None:
                trade_command["tp"] = tp

            response = self.client.commandExecute("tradeTransaction", trade_command)

            if response['status']:
                return response['returnData']
            else:
                logger.error(f"Error opening trade: {response['errorDescr']}")
                return None
        except Exception as e:
            logger.error(f"Exception while opening trade: {e}")
            return None
    async  def get_all_symbols(self):
        """
        Fetches all available symbols using xStation 5 API.

        Returns:
            list: A list of symbols, or None if an error occurs.
        """
        try:
            response = self.client.commandExecute("getAllSymbols", {})
            if response['status']:
                return response['returnData']
            else:
                logger.error(f"Failed to fetch symbols. Error code: {response['errorCode']}")
                return None
        except Exception as e:
            logger.error(f"Error fetching all symbols: {e}")
            return None

    def get_chart_range(self, symbol: str, period: str, start: int, end: int):
        """
        Fetches candlestick data for a given symbol using xStation 5 API.

        Args:
            symbol (str): The symbol of the product (e.g., 'EURUSD').
            period (str): The period of the candles (e.g., '1m', '1h', '1d').
            start (int): The start timestamp for the data.
            end (int): The end timestamp for the data.

        Returns:
            list: A list of candlestick data, or None if an error occurs.
        """
        try:
            response = self.client.commandExecute("getChartRangeRequest", {
                "info": {
                    "period": period,
                    "start": start,
                    "end": end,
                    "symbol": symbol
                }
            })

            if response['status']:
                return response['returnData']
            else:
                logger.error(f"Failed to fetch candlestick data for {symbol}. Error code: {response['errorCode']}")
                return None
        except Exception as e:
            logger.error(f"Error fetching candlestick data for {symbol}: {e}")
            return None
    def get_chart_last(self, symbol: str, period: str, start: int):
        """
        Fetches the most recent candlestick data for a given symbol using xStation 5 API.

        Args:
            symbol (str): The symbol of the product (e.g., 'EURUSD').
            period (str): The period of the candles (e.g., '1m', '1h', '1d').
            start (int): The start timestamp for the data.

        Returns:
            dict: The most recent candlestick data, or None if an error occurs.
        """
        try:
            response = self.client.commandExecute("getChartLastRequest", {
                "info": {
                    "period": period,
                    "start": start,
                    "symbol": symbol
                }
            })

            if response['status']:
                return response['returnData']
            else:
                logger.error(f"Failed to fetch the most recent candlestick data for {symbol}. Error code: {response['errorCode']}")
                return None
        except Exception as e:
            logger.error(f"Error fetching the most recent candlestick data for {symbol}: {e}")
            return None
        
    def get_chart_range(self, symbol: str, period: str, start: int, end: int):
        """
        Fetches candlestick data for a given symbol using xStation 5 API.

        Args:
            symbol (str): The symbol of the product (e.g., 'EURUSD').
            period (str): The period of the candles (e.g., '1m', '1h', '1d').
            start (int): The start timestamp for the data.
            end (int): The end timestamp for the data.

        Returns:
            list: A list of candlestick data, or None if an error occurs.
        """
        try:
            response = self.client.commandExecute("getChartRangeRequest", {
                "info": {
                    "period": period,
                    "start": start,
                    "end": end,
                    "symbol": symbol
                }
            })

            if response['status']:
                return response['returnData']
            else:
                logger.error(f"Failed to fetch candlestick data for {symbol}. Error code: {response['errorCode']}")
                return None
        except Exception as e:
            logger.error(f"Error fetching candlestick data for {symbol}: {e}")
            return None

  