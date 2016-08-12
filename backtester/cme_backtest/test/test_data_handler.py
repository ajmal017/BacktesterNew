import unittest
import datetime as dt
from cme_backtest.data_handler import CMEBacktestDataHandler
from Queue import Queue

class TestCMEBacktest(unittest.TestCase):

    @classmethod
    def setUpCls(cls):
        cls.events = Queue()

if __name__ == "__main__":
    events = Queue()
    symbols = ['CLH6']
    start_date = dt.datetime(year=2016, month=1, day=15)
    end_date = dt.datetime(year=2016, month=1, day=16)
    data_handler = CMEBacktestDataHandler(events, symbols, start_date, end_date)
    data_handler.update()
    foo = data_handler.get_latest()