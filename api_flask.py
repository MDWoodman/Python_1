import flask as fl
from flask import Flask, request, jsonify
from conf import API_HOST, API_PORT
import database as db
import conf as cnf

app = Flask(__name__)

@app.route('/api', methods=['GET'])
def api():
    return jsonify({"message": "Hello, World!"})

@app.route('/api/get_symbols_to_open', methods=['GET'])
def get_symbols_to_open_transaction():
    # This function should return a list of symbols to open transactions
    result = db.get_transations_from_database(cnf.PERIOD , "OPEN")
    # extract symbols and buy_sell
    symbols = [{"symbol": tx[1], "buy_sell": tx[3]} for tx in result]
    return jsonify({"symbols": symbols})

@app.route('/api/send_symbols_to_open', methods=['POST'])
def send_symbols_to_open_transaction(signals):
    # signals includes a list of symbols to open transactions witch to parameters symbols and buy sell
    for signal in signals:
        symbol = signal["symbol"]
        buy_sell = signal["buy_sell"]
        print(f"Sending symbol to open transaction: {symbol}, Buy/Sell: {buy_sell}")
        # Here you would add the code to send the symbol and buy/sell to the trading platform

@app.route('/api/get_symbols_to_close', methods=['GET'])
def get_symbols_to_close_transaction():
    # This function should return a list of symbols to close transactions
    result = db.get_transations_from_database(cnf.PERIOD , "CLOSE")
    # extract symbols and buy_sell
    symbols = [{"symbol": tx[1], "buy_sell": tx[3]} for tx in result]
    return jsonify({"symbols": symbols})

@app.route('/api/send_symbols_to_close', methods=['POST'])
def send_symbols_to_close_transaction(signals):
    # signals includes a list of symbols to close transactions witch to parameters symbols and buy sell
    for signal in signals:
        symbol = signal["symbol"]
        buy_sell = signal["buy_sell"]
        print(f"Sending symbol to close transaction: {symbol}, Buy/Sell: {buy_sell}")
        # Here you would add the code to send the symbol and buy/sell to the trading platform

if __name__ == '__main__':
    app.run(debug=True, host=API_HOST, port=API_PORT)
