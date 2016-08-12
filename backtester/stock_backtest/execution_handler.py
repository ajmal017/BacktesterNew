import numpy as np
import utils.yahoo_finance as yf
from events import StockBacktestFillEvent
from trading.execution_handler import ExecutionHandler

COMMISSION = 5.0
PRICE_FIELD = 'Open'  # which O/H/L/C field to fill orders with


class StockBacktestExecutionHandler(ExecutionHandler):

    def __init__(self, events):
        """
        Handles simulated execution using inter-day data.
        :param events: (Queue)
        """
        super(StockBacktestExecutionHandler, self).__init__(events)
        self.symbol_data = {}
        self.resting_orders = []
        self.curr_dt = None

    def process_new_order(self, order_event):
        self._check_symbol_data(order_event.symbol)
        self._place_order(order_event)

    def process_resting_orders(self, market_event):
        """
        Got a new market update, check if we can fill any resting orders.
        :param market_event: (StockBacktestMarketEvent)
        """
        self.curr_dt = market_event.dt
        for resting_order in self.resting_orders:
            self._process_limit_order(resting_order)

    def _place_order(self, order_event):
        """
        Handles either a MARKET or LIMIT order.
        :param order_event: (OrderEvent)
        :return:
        """
        order_handlers = {
            'MARKET': self._fill_market_order,
            'LIMIT': self._process_limit_order
        }
        order_handlers[order_event.order_type](order_event)

    def _process_limit_order(self, order_event, resting=False):
        """
        :param order_event:
        :return:
        """
        order_direction = np.sign(order_event.quantity)
        if order_direction == 1:
            if self.symbol_data[order_event.symbol].ix[self.curr_dt][PRICE_FIELD] \
                    <= order_event.price:
                self._fill_market_order(order_event)
                return

        elif order_direction == -1:
            if self.symbol_data[order_event.symbol].ix[self.curr_dt][PRICE_FIELD] \
                    >= order_event.price:
                self._fill_market_order(order_event)
                return

        if not resting:
            self.resting_orders.append(order_event)

    def _check_symbol_data(self, symbol):
        """
        Load all data for a symbol if haven't done so already.
        :param symbol: (str)
        """
        if symbol not in self.symbol_data:
           self.symbol_data[symbol] = yf.get_stock_data(symbol)


    def _fill_market_order(self, order_event):
        """
        Fills an order at the current market price and puts the fill event into the queue.
        :param order_event: (OrderEvent)
        """
        if order_event.quantity == 0:
            return
        sym_data = self.symbol_data[order_event.symbol].ix[order_event.order_time]
        fill_price = sym_data[PRICE_FIELD]
        fill_event = self.create_fill_event(order_event, fill_price, order_event.order_time)
        self.events.put(fill_event)

    def create_fill_event(self, order_event, fill_price, fill_time):
        """
        Make a fill event and put it back into the events queue.
        :param order_event:
        :param fill_price: (float)
        :param fill_time: (DateTime)
        :return: (StockBacktestFillEvent)
        """
        fill_event = StockBacktestFillEvent(self.curr_dt, order_event.symbol, order_event.quantity,
                                            fill_price, fill_price*order_event.quantity,
                                            commission=COMMISSION)
        self.events.put(fill_event)

