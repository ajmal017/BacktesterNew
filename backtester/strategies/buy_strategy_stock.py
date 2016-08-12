from random import randint
from collections import OrderedDict
from trading.events import OrderEvent
from trading.stock_strategy import StockStrategy


class BuyStrategy(StockStrategy):
    def __init__(self, events, data, products, initial_cash=1000000):
        super(BuyStrategy, self).__init__(events, data, products, initial_cash)
        self.curr_dt = None
        self.prod1 = products[0]
        self.sym1 = products[0].symbol
        self.fills = []

    def new_tick(self):
        sym1_order_qty = randint(-100, 100)
        temp_capital = self.cash
        if self._check_order(temp_capital, self.sym1, sym1_order_qty):
            self.order(self.prod1, sym1_order_qty, order_time=self.curr_dt)
            temp_capital -= self.last_bar[self.sym1][self.price_field] * sym1_order_qty

    def _check_order(self, capital, symbol, quantity):
        if self.last_bar[symbol][self.price_field] * quantity < capital:
            return True
        return False

    def new_fill(self, fill_event):
        pass
