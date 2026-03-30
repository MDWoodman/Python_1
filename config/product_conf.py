import os
import json
import asyncio
import database as db

#generate class ProductConf with parameters symbol
class ProductConf:
    def __init__(self, symbol , short_window_mcad, long_window_mcad , signal_window_mcad , angle_mcad, adx_window, adx_adx , tenkansen_period, kijunsen_period, senkouspan_period):
        self.symbol = symbol
        self.short_window_mcad = short_window_mcad
        self.long_window_mcad = long_window_mcad
        self.signal_window_mcad = signal_window_mcad
        self.angle_mcad = angle_mcad
        self.adx_window = adx_window
        self.adx_adx= adx_adx
        self.tenkansen_period = tenkansen_period
        self.kijunsen_period = kijunsen_period
        self.senkouspan_period = senkouspan_period

    def to_dict(self):
        return {
            'symbol': self.symbol,
            'short_window_mcad': self.short_window_mcad,
            'long_window_mcad': self.long_window_mcad,
            'signal_window_mcad': self.signal_window_mcad,
            'angle_mcad': self.angle_mcad,
            'adx_window': self.adx_window,
            'adx_adx': self.adx_adx,
            'tenkansen_period': self.tenkansen_period,
            'kijunsen_period': self.kijunsen_period,
            'senkouspan_period': self.senkouspan_period
        }
    @staticmethod
    def load_products_from_json(file_path):

        with open(file_path, 'r') as file:
            data = json.load(file)
        
        products = []
        for item in data:
            product = ProductConf(**item)
            products.append(product)
        
        return products
    
    @staticmethod
    def find_product_by_symbol(products, symbol):
        for product in products:
            if product.symbol == symbol:
                return product
        return None
   
# get ssId from login response

class ProductDB:
    def __init__(self , symbol : str , name : str = '', risk_price : float = 0.0, SL : float = 0.0, volume : float = 0.0):
       self.symbol = symbol
       self.name = name
       self.risk_price = risk_price
       self.SL = SL
       self.volume = volume

    @staticmethod
    def load_productsDB_from_json(file_path):

        with open(file_path, 'r') as file:
            data = json.load(file)
        
        products = []
        for item in data:
            product = ProductDB(item['symbol'])
            if 'symbol' in item:
                product.symbol = item['symbol']
            if 'name' in item:
                product.name = item['name']
            if 'risk_price' in item:
                product.risk_price = item['risk_price']
            if 'SL' in item:
                product.SL = item['SL']
            if 'volume' in item:
                product.volume = item['volume']

            products.append(product)
        
        return products
    @staticmethod
    def find_productDB_by_symbol(products, symbol):
        for product in products:
            if product.symbol == symbol:
                return product
        return None
    def get_product(self, symbol):
        try:
            prod =  db.get_product_from_database(symbol)
            self.symbol = prod[0]
            self.name = prod[1]
            self.risk_price = prod[2]
            self.SL = prod[3]
            self.volume = prod[4]
        except Exception as e:
            print(e)

        return self

    def save_product(self):
        try:
            db.save_product_to_database(self.symbol, self.name, self.risk_price, self.SL, self.volume)
         
        except Exception as e:
            print(e)
