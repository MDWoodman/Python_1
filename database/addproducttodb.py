from product_conf import ProductDB
from product_conf import ProductConf
import conf as cnf
import os


for symbol in cnf.SYMBOLS_LIST:
  
    products_list = ProductDB.load_productsDB_from_json(os.path.dirname(os.path.abspath(__file__)) + '\\productsDB.json')
    product = ProductDB.find_productDB_by_symbol(products_list, symbol)
    if product:

        productdb = ProductDB(symbol, product.name, product.risk_price, product.SL, product.volume)
        productdb.save_product()
