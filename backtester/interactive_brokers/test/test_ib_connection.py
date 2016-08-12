import unittest
import time
import json
from queue import Queue
from interactive_brokers.ib_connection import IBConnection
from interactive_brokers.ib_execution_handler import create_order
from interactive_brokers.ib_utils import create_ib_futures_contract
from ib.ext.Order import Order

CONFIG = json.load(open('test_ib_config.json', 'r'))
ACCOUNT = CONFIG['ACCOUNT']
PORT = CONFIG['PORT']
CLIENT_ID = CONFIG['CONNECTION_CLIENT_ID']
PAPER_ACCOUNT = CONFIG['PAPER_ACCOUNT']

class TestIBConnection(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.events = Queue()
        cls.port = PORT
        cls.client_id = CLIENT_ID
        cls.ib_connection = IBConnection(cls.port, cls.events, cls.client_id)
        while(cls.ib_connection.next_valid_order_id is -1): # or cls.ib_connection.account is not PAPER_TRADING_ACCOUNT):
            time.sleep(.1)

    def test_connect_ib(self):
        """
        Trader WorkStation or Gateway must be running.
        :return:
        """
        self.assertTrue(self.ib_connection.is_connected())
        self.assertEqual(self.ib_connection.account, PAPER_ACCOUNT)

    def test_handle_exec_details(self):

        # Create an order and send it
        contract = create_ib_futures_contract('GC', exp_month=5, exp_year=2016)
        order = create_order('MKT', 1, limit_price=None)
        order = Order()
        order.m_action = 'BUY'
        order.m_totalQuantity = 1
        order.m_orderType = 'MKT'
        self.ib_connection.place_order(contract, order)
        time.sleep(3)


