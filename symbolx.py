import json
from typing import Optional

class SymbolX:
    def __init__(self, ask: float, bid: float, categoryName: str, contractSize: int, currency: str, currencyPair: bool,
                 currencyProfit: str, description: str, expiration: Optional[str], groupName: str, high: float,
                 initialMargin: int, instantMaxVolume: int, leverage: float, longOnly: bool, lotMax: float, lotMin: float,
                 lotStep: float, low: float, marginHedged: int, marginHedgedStrong: bool, marginMaintenance: Optional[str],
                 marginMode: int, percentage: float, precision: int, profitMode: int, quoteId: int, shortSelling: bool,
                 spreadRaw: float, spreadTable: float, starting: Optional[str], stepRuleId: int, stopsLevel: int,
                 swap_rollover3days: int, swapEnable: bool, swapLong: float, swapShort: float, swapType: int, symbol: str,
                 tickSize: float, tickValue: float, time: int, timeString: str, trailingEnabled: bool, type: int):
        self.ask = ask
        self.bid = bid
        self.categoryName = categoryName
        self.contractSize = contractSize
        self.currency = currency
        self.currencyPair = currencyPair
        self.currencyProfit = currencyProfit
        self.description = description
        self.expiration = expiration
        self.groupName = groupName
        self.high = high
        self.initialMargin = initialMargin
        self.instantMaxVolume = instantMaxVolume
        self.leverage = leverage
        self.longOnly = longOnly
        self.lotMax = lotMax
        self.lotMin = lotMin
        self.lotStep = lotStep
        self.low = low
        self.marginHedged = marginHedged
        self.marginHedgedStrong = marginHedgedStrong
        self.marginMaintenance = marginMaintenance
        self.marginMode = marginMode
        self.percentage = percentage
        self.precision = precision
        self.profitMode = profitMode
        self.quoteId = quoteId
        self.shortSelling = shortSelling
        self.spreadRaw = spreadRaw
        self.spreadTable = spreadTable
        self.starting = starting
        self.stepRuleId = stepRuleId
        self.stopsLevel = stopsLevel
        self.swap_rollover3days = swap_rollover3days
        self.swapEnable = swapEnable
        self.swapLong = swapLong
        self.swapShort = swapShort
        self.swapType = swapType
        self.symbol = symbol
        self.tickSize = tickSize
        self.tickValue = tickValue
        self.time = time
        self.timeString = timeString
        self.trailingEnabled = trailingEnabled
        self.type = type

    @classmethod
    def DeserialaizeSymbolX(cls,symbols_data):
        symbols=[SymbolX(
        ask=symbol["ask"], 
        bid=symbol["bid"], 
        categoryName=symbol["categoryName"], 
        contractSize=symbol["contractSize"], 
        currency=symbol["currency"], 
        currencyPair=symbol["currencyPair"], 
        currencyProfit=symbol["currencyProfit"], 
        description=symbol["description"], 
        expiration=symbol["expiration"], 
        groupName=symbol["groupName"], 
        high=symbol["high"], 
        initialMargin=symbol["initialMargin"], 
        instantMaxVolume=symbol["instantMaxVolume"], 
        leverage=symbol["leverage"], 
        longOnly=symbol["longOnly"], 
        lotMax=symbol["lotMax"], 
        lotMin=symbol["lotMin"], 
        lotStep=symbol["lotStep"], 
        low=symbol["low"], 
        marginHedged=symbol["marginHedged"], 
        marginHedgedStrong=symbol["marginHedgedStrong"], 
        marginMaintenance=symbol["marginMaintenance"], 
        marginMode=symbol["marginMode"], 
        percentage=symbol["percentage"], 
        precision=symbol["precision"], 
        profitMode=symbol["profitMode"], 
        quoteId=symbol["quoteId"], 
        shortSelling=symbol["shortSelling"], 
        spreadRaw=symbol["spreadRaw"], 
        spreadTable=symbol["spreadTable"], 
        starting=symbol["starting"], 
        stepRuleId=symbol["stepRuleId"], 
        stopsLevel=symbol["stopsLevel"], 
        swap_rollover3days=symbol["swap_rollover3days"], 
        swapEnable=symbol["swapEnable"], 
        swapLong=symbol["swapLong"], 
        swapShort=symbol["swapShort"], 
        swapType=symbol["swapType"], 
        symbol=symbol["symbol"], 
        tickSize=symbol["tickSize"], 
        tickValue=symbol["tickValue"], 
        time=symbol["time"], 
        timeString=symbol["timeString"], 
        trailingEnabled=symbol["trailingEnabled"], 
        type=symbol["type"]
        ) for symbol in symbols_data] 

        return symbols