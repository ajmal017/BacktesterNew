from abc import ABCMeta, abstractmethod


class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (O/H/L/C/V) for each symbol requested.

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the trading system.
    """

    __metaclass__ = ABCMeta

    def __init__(self, events, **kwargs):
        self.events = events

    @abstractmethod
    def update(self):
        """
        Updates self.last_tick
        Also should send a MarketEvent to the queue.
        """
        raise NotImplementedError("DataHandler.update_bars()")

class BacktestDataHandler(DataHandler):
    def __init__(self, events, products, start_date, end_date, start_time=None, end_time=None):
        super(BacktestDataHandler, self).__init__(events)
        self.continue_backtest = True
        self.products = products
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = start_time
        self.end_time = end_time