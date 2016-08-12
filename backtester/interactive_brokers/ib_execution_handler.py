import logging
import os
import sys
import time
import threading
from collections import deque
from ib.ext.Order import Order
from trading.execution_handler import ExecutionHandler
from ib_connection import IBConnection
from ib_utils import get_contract_details, get_execution_details, create_ib_futures_contract_from_symbol
from ib_events import IBFillEvent

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s')
log = logging.getLogger('IBExecutionHandler')


class IBExecutionHandler(ExecutionHandler, IBConnection):

    def __init__(self, events, config):
        self.events = events
        self.port = config['PORT']
        self.client_id = config['EXECUTION_CLIENT_ID']
        ExecutionHandler.__init__(self, self.events)
        IBConnection.__init__(self, self.events, self.port, self.client_id)

        # Reply handler thread
        thread = threading.Thread(target=self._reply_handler, args=())
        thread.daemon = True
        thread.start()

        self.orders = {}
        self.fills = deque()

        log.info("IBExecutionHandler initialized!")

    def process_new_order(self, order_event):
        """
        Processes an IBOrderEvent (called from ), creates an (ib.ext.Order) and sends it to IB.
        LiveTrade fills the contract param.
        :param order_event: (IBOrderEvent)
        :param contract: (ib.ext.Contract)
        """
        order = create_order(order_event.order_type, order_event.quantity, limit_price=order_event.price)
        contract = order_event.product.ib_contract
        self._send_order(contract, order)

    def _send_order(self, contract, order):
        """
        Sends the order to IB through the connection.
        (Wrapper for placeOrder(), id is automatically generated from TWS.)
        :param contract: (ib.ext.Contract)
        :param order: (ib.ext.Order)
        :return: (bool) True on completion
        """
        order_id = self.next_valid_order_id
        self.next_valid_order_id += 1
        self.connection.placeOrder(order_id, contract, order)
        self.orders[order_id] = (contract, order)

    def _log_order(self, contract, order):
        raise NotImplementedError

    def _reply_handler(self):
        """
        Handle all type of replies from IB in a separate thread.
        :return:
        """
        reply_handlers = {
            'connectionClosed': super(IBExecutionHandler, self).handle_connection_closed_msg,
            'error': super(IBExecutionHandler, self).handle_error_msg,
            'managedAccounts': super(IBExecutionHandler, self).handle_managed_accounts_msg,
            'nextValidId': super(IBExecutionHandler, self).handle_next_valid_id_msg,
            'execDetails': self.handle_exec_details_msg,
            'openOrder': self.handle_open_order_msg,
            'orderStatus': self.handle_order_status_msg,
            'commissionReport': self.handle_commission_report_msg
        }

        while True:
            try:
                msg = self.messages.popleft()
                try:
                    msg_dict = dict(msg.items())
                    if msg.typeName == 'connectionClosed':
                        print 'connection closed fk'
                    msg_dict['typeName'] = msg.typeName
                    event = reply_handlers[msg.typeName](msg_dict)  # format message as dict
                    self.events.put(event)
                except KeyError:
                    print msg.typeName, 'NEED TO HANDLE THIS KIND OF MESSAGE'
            except IndexError:
                time.sleep(self.msg_interval)

    def handle_exec_details_msg(self, msg):
        """
        Parses execution details
        :param msg:
        :return: IBFillEvent
        """
        execution = get_execution_details(msg['execution'])
        contract = get_contract_details(msg['contract'])
        ib_fill_event = IBFillEvent(execution, contract)
        self.fills.append(ib_fill_event)
        return ib_fill_event

    def handle_open_order_msg(self, msg):
        print 'handle_open_order_msg', msg
        pass

    def handle_order_status_msg(self, msg):
        print 'handle_order_status_msg', msg
        pass

    def handle_commission_report_msg(self, msg):
        print 'handle_commission_report_msg', msg
        pass

def create_order(order_type, quantity, limit_price=None):
    """
    Creates an (ib.ext.Order) object to send to IB
    :param order_type: (string) "MARKET" or "LIMIT"
    :param quantity: (int)
    :return: (ib.ext.Order)
    """
    order = Order()
    if order_type is "MARKET":
        order.m_orderType = "MKT"
    elif order_type is "LIMIT":
        order.m_orderType = "LMT"
        order.m_lmtPrice = limit_price
    assert(order.m_orderType is not None), "Invalid order_type!"

    if quantity == 0:
        raise Exception('Order quantity is 0!')
    elif quantity > 0:
        order.m_action = "BUY"
    elif quantity < 0:
        order.m_action = "SELL"
    assert(order.m_action is not None), "Invalid order action!"
    order.m_totalQuantity = abs(quantity)
    assert(abs(order.m_totalQuantity) > 0), "Invalid order quantity!"

    return order
