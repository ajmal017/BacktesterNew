import json
import time
import unittest
from queue import Queue
from trading.futures_contract import FuturesContract
from interactive_brokers.ib_data_handler import IBDataHandler

CONFIG = json.load(open('test_ib_config.json', 'r'))


class TestIBDataHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.events = Queue()
        cls.products = [FuturesContract('GC', exp_year=2016, exp_month=6)]
        cls.gold_symbol = cls.products[0].symbol
        cls.data = IBDataHandler(cls.events, cls.products, CONFIG)
        while cls.data.next_valid_order_id is -1:
            time.sleep(.1)

    def test_data_stream(self):
        time.sleep(1)
        last_tick = self.data.last_bar
        for i in range(100):
            print last_tick
            self.assertGreater(last_tick[self.gold_symbol]['level_1_price_sell'],
                               last_tick[self.gold_symbol]['level_1_price_buy'])
            time.sleep(1)
