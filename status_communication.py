from database import database as db
from config import conf as cnf
import email_msg as em

def check_get_signal_to_open_transaction(trade: str , symbol:str) -> bool:
    db_result = db.get_signal_to_open_transaction(trade, symbol , "OPEN TRANSACTION")
   # em.check_email_for_signal(f"OPEN TRANSACTION - {trade} - {symbol}")

    if not db_result:
        return False
    return True
def check_if_transaction_is_opened(trade: str, symbol: str) -> bool:
    db_result = db.get_signal_to_open_transaction(trade, symbol, "OPENED")

   # em.check_email_for_signal(f"OPENED - {trade} - {symbol}")

    if not db_result:
        return False
    return True
def update_signal_to_open_transaction(trade :str, symbol:str) -> None:
    db.update_signal_to_open_transaction(trade, symbol, "OPENED")
   # em.create_and_send_email(
    #    f"OPENED - {trade} - {symbol}",
   #     f"Transaction for {trade} on {symbol} has been opened."
   # )

def send_signal_to_open_transaction(trade: str, symbol: str) -> None:
    sending = db.set_signal_to_open_transaction(trade, symbol, "TO OPEN")
    if sending:
        em.create_and_send_email(
            f"TO OPEN - {trade} - {symbol}",
            f"Signal to open transaction for {trade} on {symbol} has been created."
            
        )
    
def send_signal_to_close_transaction(trade: str, symbol: str) -> None:
    db.set_signal_to_close_transaction(trade, symbol, "TO CLOSE")
   
def check_get_signal_to_close_transaction(trade: str, symbol: str) -> bool:
    db_result = db.get_signal_to_close_transaction(trade, symbol, "TO CLOSE")
    if not db_result:
        return False
    return True

def send_api_request_to_open_transaction(trade: str, symbol: str) -> None:
    db.set_signal_to_open_transaction(trade, symbol, "TO OPEN")
  

def check_api_signal_to_open_transaction(trade: str, symbol: str) -> bool:
    db_result = db.get_signal_to_open_transaction(trade, symbol, "TO OPEN")
    if not db_result:
        return False
    return True

def update_api_transaction_status(trade: str, symbol: str, status: str) -> None:
    db.update_signal_to_open_transaction(trade, symbol, status)
   