import pandas as pd
from abc import ABCMeta, abstractmethod
from trading.events import OrderEvent


class Strategy(object):

    __metaclass__ = ABCMeta

    def __init__(self, events, data, products, initial_cash, *args, **kwargs):
        """
        The Strategy is an ABC that presents an interface for taking market data and
        generating corresponding OrderEvents which are sent to the ExecutionHandler.

        :param events: (Queue)
        :param data: (DataHandler)
        :param products: (list) (FuturesContract)
        :param initial_cash: (float)
        :param args:
        :param kwargs:
        """
        self.events = events
        self.data = data
        self.products = products
        self.curr_dt = None
        self.positions = {product.symbol: 0 for product in self.products}
        self.positions_series = {product.symbol: pd.DataFrame(data=None, columns=['dt', 'pos'])
                                 for product in self.products}
        self.transactions_series = pd.DataFrame(data=None, columns=['dt', 'amount', 'price', 'symbol'])
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.curr_pnl = 0
        self.last_bar = None
        self.initialize(*args, **kwargs)

    def order(self, product, quantity, order_type='MARKET', price=None, order_time=None):
        """
        Generate an order and place it into events.
        :param product: (FuturesContract)
        :param order_type: (str) 'MARKET' or 'LIMIT'
        :param quantity: (int)
        :param price: (float)
        :param order_time: (DateTime)
        """
        order_time = order_time if order_time is not None else self.curr_dt
        order = OrderEvent(product, quantity, order_type, price, order_time)
        self.events.put(order)

    @abstractmethod
    def new_tick_update(self, market_event):
        """
        Internal updates on new tick.
        Updates positions, values, time series, returns, etc.
        :param market_event: (MarketEvent)
        """
        raise NotImplementedError('Strategy.new_tick_update()')

    @abstractmethod
    def new_tick(self):
        """
        Call back for when the strategy receives a new tick.
        self.last_bar is automatically updated before this.
        """
        raise NotImplementedError('Strategy.new_tick()')

    def new_fill_update(self, fill_event):
        """
        Updates:
            self.positions
            self.cash
            self.transactions_series
        :param fill_event: (FillEvent)
        """

        raise NotImplementedError(Strategy.new_fill_update())
            # self.positions[fill_event.symbol] += fill_event.quantity
        # self.cash -= fill_event.fill_cost
        #
        # transaction = [fill_event.fill_time, fill_event.quantity, fill_event.fill_price, fill_event.symbol]
        # self.transactions_series[fill_event.symbol].loc[len(self.transactions_series)] = transaction



    @abstractmethod
    def new_fill(self, fill_event):
        """
        Call back for when an order placed by the strategy is filled.
        Updated before this callback:
            self.positions
            self.cash
            self.transactions_series
        :param fill_event: (FillEvent)
        :return:
        """
        raise NotImplementedError("Strategy.new_fill()")

    @abstractmethod
    def finished(self):
        """
        Call back for when a backtest (or live-trading) is finished.
        Creates the time_series (DataFrame)
        """
        raise NotImplementedError("Strategy.finished()")

    def initialize(self, *args, **kwargs):
        """
        Initialize the strategy
        """
        pass

    # @abstractmethod
    # def new_day(self, event):
    #     """
    #     Call back for when the strategy receives a tick that is a new day.
    #     :param event:
    #     :return:
    #     """
    #     raise NotImplementedError("new_day()")
