from datetime import datetime, timedelta
import os
from symtable import Symbol
import time
import login as login
import conf as cnf
import logging as logger
import tools as tools
from api import API
from candle import Candle
from symbolx import SymbolX
import database as db


log_file_path = tools.logger_configuration()


logger.basicConfig(filename=log_file_path, level=logger.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

api = API(cnf.username, cnf.password)

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

import asyncio
from datetime import datetime
from loadBaseData import load_base_data

load_base_data(api)