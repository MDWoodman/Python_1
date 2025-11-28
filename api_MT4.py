from APIConnector_MT4 import DWX_ZeroMQ_Connector
import time





class API:

    def __init__(self) -> None:

        self.zmq =  DWX_ZeroMQ_Connector()
       

       

    def get_chart_range(self, symbol: str, period: str, start: int, end: int):
        """
        Fetches candlestick data for a given symbol using xStation 5 API.

        Args:
            symbol (str): The symbol of the product (e.g., 'EURUSD').
            period (str): The period of the candles (e.g., '1m', '1h', '1d').
            start (int): The start timestamp for the data.
            end (int): The end timestamp for the data.

        Returns:
            dict: Candlestick data, or None if an error occurs.
        """
        try:
             self.zmq._DWX_MTX_SEND_HIST_REQUEST_( _symbol=symbol, _timeframe=period, _start=-start, _end=end )

            
             time.sleep(0.5)  # Wait for the data to be received
             response = self.zmq._get_response_()
             if response is not None:
                data = response.get('_data', None)
                if data is not None:
                    return data
                else:
                    # logger.error(f"Failed to fetch candlestick data for {symbol}.")
                    return None
             else:
                # logger.error(f"No response received for {symbol}.")
                return None
            
        except Exception as e:
            # logger.error(f"Error fetching candlestick data for {symbol}: {e}")
            return None

    def open_transaction(self , action , _type,symbol, price , stop_loss ,take_profit , comment , lot_size , magic , ticket):
        """
        Opens a new transaction (trade) using DWX_ZeroMQ_Connector.

        Args:
            action (str): The action to perform ('buy' or 'sell').
            _type (str): The type of order ('market' or 'pending').
            symbol (str): The symbol of the product (e.g., 'EURUSD').
            price (float): The price at which to open the trade.
            stop_loss (float): The stop loss price.
            take_profit (float): The take profit price.
            comment (str): Order comment.
            lot_size (float): The size of the trade in lots.
            magic (int): Magic number for the order.
            ticket (int): Ticket number for the order.

        Returns:
            dict: The response from the connector, or None if an error occurs.
        """
        try:
            order =self._generate_order_dict(action, _type, symbol, price, stop_loss, take_profit, comment, lot_size, magic, ticket)
            

            self.zmq._DWX_MTX_NEW_TRADE_(_order=order)
            
            time.sleep(0.5)  # Wait for the data to be received
            response = self.zmq._get_response_()
            return response

        except Exception as e:
            # logger.error(f"Error opening transaction: {e}")
            return None
    def get_symbol_lot_info(self, symbol: str):
            self.zmq._DWX_MTX_GET_SYMBOL_INFO_(_symbol=symbol)
            time.sleep(0.5)
            response = self.zmq._get_response_()
            if response is not None:
                info = response.get('_data', None)
                if info:
                    return {
                        "min_lot": info.get('volume_min', 0.01),
                        "max_lot": info.get('volume_max', 100.0),
                        "lot_step": info.get('volume_step', 0.01)
                    }
            return {"min_lot": 0.01, "max_lot": 100.0, "lot_step": 0.01}    
   
    def _generate_order_dict(self , action: str, _type: str, symbol: str, price: float, stop_loss: float, take_profit: float, comment: str, lots: float, magic: int, ticket: int):
        return({'_action': action,
                  '_type': _type,
                  '_symbol': symbol,
                  '_price': price,
                  '_SL': stop_loss, # SL/TP in POINTS, not pips.
                  '_TP': take_profit,
                  '_comment': comment,
                  '_lots': lots,
                  '_magic': magic,
                  '_ticket': ticket})
    def close_transaction(self, ticket: int, volume: float = None, comment: str = ""):
        """
        Closes an open transaction (trade) by ticket number.

        Args:
            ticket (int): The ticket number of the trade to close.
            volume (float, optional): The volume to close. If None, closes the full position.
            comment (str, optional): Order comment.

        Returns:
            dict: The response from the connector, or None if an error occurs.
        """
        try:
            close_order = {
                'ticket': ticket,
                'type': 'close',
                'comment': comment
            }
            if volume is not None:
                close_order['volume'] = volume

            self.zmq._DWX_MTX_CLOSE_TRADE_BY_TICKET_(_ticket=ticket, _lots=volume)
            time.sleep(0.5)
            response = self.zmq._get_response_()
            return response

        except Exception as e:
            # logger.error(f"Error closing transaction {ticket}: {e}")
            return None
    def get_last_candle(self, symbol: str, period: str):
        """
        Fetches the last candlestick data for a given symbol and period using DWX_ZeroMQ_Connector.

        Args:
            symbol (str): The symbol of the product (e.g., 'EURUSD').
            period (str): The period of the candles (e.g., '1m', '1h', '1d').

        Returns:
            dict: The last candlestick data, or None if an error occurs.
        """
        try:
            # Request the last candle
            self.zmq._DWX_MTX_SEND_HIST_REQUEST_(_symbol=symbol, _timeframe=period, _start=-1, _end=0)

            time.sleep(0.5)  # Wait for the data to be received
            response = self.zmq._get_response_()

            if response is not None:
                data = response.get('_data', None)
                if data and len(data) > 0:
                    return data[-1]  # Return the last candle
                else:
                    # logger.error(f"Failed to fetch the last candle for {symbol}.")
                    return None
            else:
                # logger.error(f"No response received for the last candle of {symbol}.")
                return None

        except Exception as e:
            # logger.error(f"Error fetching the last candle for {symbol}: {e}")
            return None