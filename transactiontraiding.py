class TransactionTrading:
    def __init__(self, symbol, period, time, type, status):
        self.symbol = symbol
        self.period = period
        self.time = time
        self.type = type
        self.status = status

    def get_symbol(self):
        return self.symbol

    def set_symbol(self, symbol):
        self.symbol = symbol

    def get_period(self):
        return self.period

    def set_period(self, period):
        self.period = period

    def get_time(self):
        return self.time

    def set_time(self, time):
        self.time = time

    def get_type(self):
        return self.type

    def set_type(self, type):
        self.type = type

    def get_status(self):
        return self.status

    def set_status(self, status):
        self.status = status