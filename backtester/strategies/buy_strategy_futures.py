from random import randint
from trading.futures_strategy import FuturesStrategy


class BuyStrategy(FuturesStrategy):
    def __init__(self, events, data, products, initial_cash=1000000):
        super(BuyStrategy, self).__init__(events, data, products, initial_cash)
        self.curr_dt = None
        self.gold = products[0]
        self.sym1 = products[0].symbol
        # self.sym2 = products[1].symbol
        self.fills = []
        self.buy_int = 0

    def new_tick(self):
        random_buy = randint(1, 10000)
        if random_buy == 444:
            random_direction = randint(0, 1)
            if random_direction == 0:
                sym1_order_qty = -1
            else:
                sym1_order_qty = 1
            temp_capital = self.cash
            if self._check_order(temp_capital, self.sym1, sym1_order_qty):
                self.order(self.gold, sym1_order_qty, order_type='MARKET', price=None, order_time=self.curr_dt)
                temp_capital -= self.last_bar[self.sym1]['level_1_price_buy'] * sym1_order_qty
                temp_capital -= self.last_bar[self.sym1]['level_1_price_buy'] * sym1_order_qty


    def _check_order(self, capital, symbol, quantity):
        if self.last_bar[symbol]['level_1_price_buy'] * quantity < capital:
            return True
        return False

    def new_fill(self, fill_event):
        pass