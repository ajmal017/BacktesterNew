import time
import logging
import threading
import datetime as dt
from trading.data_handler import DataHandler
from trading.events import MarketEvent
from interactive_brokers.ib_connection import IBConnection
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s')
log = logging.getLogger('IBDataHandler')


class IBDataHandler(DataHandler, IBConnection):

    def __init__(self, events, products, config):
        self.events = events
        self.products = products
        self.port = config['PORT']
        self.client_id = config['DATA_CLIENT_ID']
        DataHandler.__init__(self, self.events)
        IBConnection.__init__(self, self.events, self.port, self.client_id)
        self._initialize_handlers()

        self.curr_dt = dt.datetime.now()
        self.last_bar = {product.symbol: {} for product in self.products}

        print 'datahandler', self.last_bar

        # Subscribe to mkt data feeds
        self.ticker_ids = {}  # ticker_id:symbol
        for i in range(len(self.products)):
            product = self.products[i]
            self.ticker_ids[i] = product.symbol
            contract = product.ib_contract
            self._req_mkt_data(i, contract)

        # Reply handler thread
        thread = threading.Thread(target=self._reply_handler, args=())
        thread.daemon = True
        thread.start()

        log.info("IBDataHandler initialized!")

    def _initialize_handlers(self):

        self.reply_handlers = {
            'error': super(IBDataHandler, self).handle_error_msg,
            'connectionClosed': super(IBDataHandler, self).handle_connection_closed_msg,
            'managedAccounts': super(IBDataHandler, self).handle_managed_accounts_msg,
            'nextValidId': super(IBDataHandler, self).handle_next_valid_id_msg,
            'tickPrice': self._handle_tick_price,
            'tickSize': self._handle_tick_size,
            'tickGeneric': self._handle_tick_generic,
            'tickString': self._handle_tick_string,
        }

        self.price_fields = {
            1:  'level_1_price_buy',   # 'bid_price',
            2:  'level_1_price_sell',  # 'ask_price',
            # 4:  'last_price',
            # 6:  'high_price',
            # 7:  'low_price',
            # 9:  'close_price',
            # 14: 'open_tick',
            # 15: 'low_13_week',
            # 16: 'high_13_week',
            # 17: 'low_26_week',
            # 18: 'high_26_week',
            # 19: 'low_52_week',
            # 20: 'high_52_week',
        }

        self.size_fields = {
            0: 'bid_size',
            3: 'ask_size',
            5: 'last_size',
            # 8: 'volume'
        }

        self.string_fields = {
            # 32: 'bid_exchange',
            # 33: 'ask_exchange',
            45: 'last_timestamp',
            # 46: 'shortable',
            # 47: 'fundamental_ratio',
            # 48: 'rt_volume',
        }

    def _req_mkt_data(self, ticker_id, contract):
        """
        tickerId (int) The ticker id. Must be a unique value. When the market data returns,
                        it will be identified by this tag. This is also used when canceling the market data.
        contract (Contract)	This class contains attributes used to describe the contract.
        genericTicklist	(String) A comma delimited list of generic tick types.  Tick types can be found in
                                the Generic Tick Types page.
        snapshot (boolean) Check to return a single snapshot of market data and have the market data
                          subscription cancel. Do not enter any genericTicklist values if you use snapshot.

        :param ticker_id:
        :param contract:
        :return:
        """
        self.connection.reqMarketDataType(1)  # type 1 is for live data
        self.connection.reqMktData(ticker_id, contract, "", False)

    def update(self):
        self.events.put(MarketEvent(self.curr_dt, self.last_bar))

    def _reply_handler(self):
        """
        Handle all type of replies from IB in a separate thread.
        :return:
        """
        while True:
            try:
                msg = self.messages.popleft()
                try:
                    msg_dict = dict(msg.items())
                    msg_dict['typeName'] = msg.typeName
                    self.reply_handlers[msg.typeName](msg_dict)  # format message as dict
                except KeyError:
                    # print "{} Need to handle message type: {}".format(repr(e), msg.typeName)
                    pass
            except IndexError:
                time.sleep(self.msg_interval)

    def _handle_tick_size(self, msg):
        """
        tickerId (int) The ticker Id that was specified previously in the call to reqMktData()
        field (int)    Specifies the type of price. Pass the field value into TickType.getField(int tickType)
                       to retrieve the field description. For example, a field value of 0 will map to bidSize,
                       a field value of 3 will map to askSize, etc.
                       0 = bid size
                       3 = ask size
                       5 = last size
                       8 = volume

        size (int) Specifies the size for the specified field
        :param msg: tickSize message
        :return:
        """
        size = msg['size']
        tick_symbol = self.ticker_ids[msg['tickerId']]
        self.last_bar[tick_symbol][self.size_fields[msg['field']]] = size

    def _handle_tick_price(self, msg):
        """
        Updates the current market price
        tickerId (int) The ticker Id that was specified previously in the call to reqMktData()
        field (int) Specifies the type of price. Pass the field value into TickType.getField(int tickType)
                    to retrieve the field description.  For example, a field value of 1 will map to bidPrice,
                    a field value of 2 will map to askPrice, etc.
                    1 = bid
                    2 = ask
                    4 = last
                    6 = high
                    7 = low
                    9 = close
        price (double) Specifies the price for the specified field
        canAutoExecute (int) Specifies whether the price tick is available for automatic execution.
                        Possible values are:
                        0 = not eligible for automatic execution
                        1 = eligible for automatic execution
        :param msg: tickPrice message
        :return: (MarketEvent)
        """
        price = msg['price']
        tick_symbol = self.ticker_ids[msg['tickerId']]
        self.last_bar[tick_symbol][self.price_fields[msg['field']]] = price

    def _handle_tick_generic(self, msg):
        """
        This method is called when the market data changes. Values are updated immediately with no delay.
        tickerId (int)	The ticker Id that was specified previously in the call to reqMktData()
        tickType (int)  Specifies the type of price.
                        Pass the field value into TickType.getField(int tickType) to retrieve the field description.
                        For example, a field value of 46 will map to shortable, etc.
        value (double)  The value of the specified field

        :param msg: tickGeneric message
        :return:
        """
        pass

    def _handle_tick_string(self, msg):
        """
        :param msg:
        :return:
        """
        string = msg['value']
        if msg['tickType'] == 45:
            string = dt.datetime.fromtimestamp(float(string))
            self.curr_dt = string
        tick_symbol = self.ticker_ids[msg['tickerId']]
        self.last_bar[tick_symbol][self.string_fields[msg['tickType']]] = string
