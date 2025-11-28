import datetime
import os
from symtable import Symbol
import time
import login as login
import conf as cnf
import logging as logger
import tools as tools
import api as api
from candle import Candle
from symbolx import SymbolX
##from database import save_to_database
import file as file
import json
from product_conf import ProductConf  # Add this line to import ProductConf

log_file_path = tools.logger_configuration()


logger.basicConfig(filename=log_file_path, level=logger.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

api = api.API(cnf.username, cnf.password)

session = api.login_xstation()
print(str(session))

logger.info(str(session)) 

if session is None:
    print('Login failed. No session returned.')
    pass
if session['status'] == False:
    print('Login failed. Error code: {0}'.format(session['errorCode']))
    pass

# get ssId from login response
ssid = session['streamSessionId']

async def main():
    symbols_data = await api.get_all_symbols()
    '''
    symb_ = symbols_data[0]
    print(symb_)
    if symbols_data:
        symbols = [SymbolX.from_json for symbol in symbols_data]
    '''
    if symbols_data:
        symbols = SymbolX.DeserialaizeSymbolX(symbols_data)
        path = os.path.dirname(os.path.abspath(__file__)) + '\\products.json'
        listOfSymbols = []
        for symbol in symbols:
            productConf = ProductConf(symbol.symbol)
            listOfSymbols.append(productConf.to_dict())
        file.save_json_to_file(json.dumps(listOfSymbols), path)
import asyncio

asyncio.run(main())

