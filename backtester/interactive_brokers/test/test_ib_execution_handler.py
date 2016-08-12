import json
import time
import unittest
import datetime as dt
from queue import Queue
from trading.events import OrderEvent
from trading.stock import Stock
from trading.futures_contract import FuturesContract
from interactive_brokers.ib_execution_handler import IBExecutionHandler
from interactive_brokers.ib_events import IBFillEvent

CONFIG = json.load(open('test_ib_config.json', 'r'))


class TestIBExecutionHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.events = Queue()
        cls.execution = IBExecutionHandler(cls.events, CONFIG)
        cls.future = FuturesContract('GC', exp_year=2016, exp_month=6)
        cls.stock = Stock('AAPL')
        while cls.execution.next_valid_order_id is -1:
            time.sleep(.1)

    def test_process_new_order_future(self):
        if self.future.mkt_close < dt.datetime.now().time() < self.future.mkt_open:
            return True
        order_event = OrderEvent(self.future, 1, 'MARKET', price=None, order_time=None)
        self.execution.process_new_order(order_event)
        time.sleep(.5)
        fill = self.execution.fills.popleft()
        self.assertIsInstance(fill, IBFillEvent)
        self.assertIsNotNone(fill)
        self.assertEqual(fill.exchange, 'NYMEX')
        self.assertEqual(fill.symbol, 'GC')
        self.assertEqual(fill.quantity, 1)
        time.sleep(.5)

    def test_process_new_order_stock(self):
        if dt.datetime.now().time() < self.stock.mkt_open or dt.datetime.now().time() > self.stock.mkt_close:
            return True
        order_event = OrderEvent(self.stock, 1, 'MARKET', price=None, order_time=None)
        self.execution.process_new_order(order_event)
        time.sleep(.5)
        # TODO: tests here
        fill = self.execution.fills.popleft()
        self.assertIsInstance(fill, IBFillEvent)
        self.assertIsNotNone(fill)
        self.assertEqual(fill.exchange, 'NYMEX')
        self.assertEqual(fill.symbol, 'GC')
        self.assertEqual(fill.quantity, 1)
        time.sleep(.5)
