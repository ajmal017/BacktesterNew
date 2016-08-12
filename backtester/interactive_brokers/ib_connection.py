import sys
import os
import logging
from collections import deque
from ib.opt import Connection
from abc import abstractmethod

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s')
log = logging.getLogger('IBConnection')


class IBConnection(object):

    def __init__(self, events, port=7495, client_id=100, msg_interval=0.1):
        self.events = events
        self.port = port
        self.client_id = client_id
        print 'ib connection', self.client_id
        self.account = ""
        self.connection = self._connect_ib()

        self.msg_interval = msg_interval
        self.messages = deque()

        self.next_valid_order_id = -1

    def _connect_ib(self):
        """
        Connect to Interactive Brokers, and set the connection.
        :return: the IB connection
        """
        tws_conn = Connection.create(port=self.port, clientId=self.client_id)
        tws_conn.connect()                                             # attempt the connection
        tws_conn.register(self.ib_error_handler, 'Error')              # assign error handler
        tws_conn.registerAll(self.ib_reply_handler)                    # assign reply handler
        assert tws_conn.isConnected() is True, "Couldn't connect to TWS!"
        log.info("Connected to Interactive Brokers on port {}.".format(self.port))
        return tws_conn

    def ib_error_handler(self, msg):
        pass

    def ib_reply_handler(self, msg):
        """
            Callback for TWS, stores messages in local deque
            :param msg:
            :return:
            """
        self.messages.append(msg)

    def is_connected(self):
        """
        Wrapper for isConnected()
        :return:
        """
        return self.connection.isConnected()

    @abstractmethod
    def _reply_handler(self):
        raise NotImplementedError()

    def handle_connection_closed_msg(self, msg):
        print "Connection closed, should probably try to reconnect (TODO)"

    def handle_error_msg(self, msg):
        """
        General IB error message handler, used by subclasses.
        :param msg:
        :return:
        """
        error_id = msg['id']
        error_code = msg['errorCode']
        error_msg = msg['errorMsg']

        error_codes = {
            2104: error_msg,  # A market data farm is connected.
            2106: error_msg,  # A historical data farm is connected
            103:  error_msg,  # error_msg + ': ' + str(error_id)  # Duplicate order id
            200:  error_msg,  # No security definition has been found for the request.
            510:  error_msg,  # Request market data - sending error:
            399:  error_msg #.replace('\n', '|'),  # Order message
        }

        try:
            log.info("{}| {}".format(error_code, error_codes[error_code]))
        except KeyError:
            print "NEED TO HANDLE ERROR CODE", error_code

    def handle_managed_accounts_msg(self, msg):
        """
        Managed accounts message handler
        :param msg:
        :return:
        """
        account = msg['accountsList']
        self.account = account
        log.info("Using account {}".format(self.account))

    def handle_next_valid_id_msg(self, msg):
        """
        Update the next valid order id as given from TWS
        :param msg:
        :return:
        """
        # TODO: this is too slow
        self.next_valid_order_id = msg['orderId']

    def __del__(self):
        """
        Destructor, disconnects from IB
        :return:
        """
        # TODO: save some shit???
        try:
            self.connection.disconnect()
            log.info("Disconnected from IB on port {}".format(self.port))
        except Exception, e:
            print "{} Error in destructor?".format(repr(e))
            pass