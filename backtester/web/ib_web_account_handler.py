import random
import time
import threading
import logging
import datetime as dt
from interactive_brokers.ib_connection import IBConnection
from interactive_brokers.ib_utils import get_contract_ticker, get_contract_details

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s')
log = logging.getLogger('IBConnection')

class IBWebAccountHandler(IBConnection):

    def __init__(self, events, config):
        self.events = events
        self.port = config['PORT']
        # self.client_id = config['PORTFOLIO_CLIENT_ID']
        self.client_id = random.randint(1000, 2000)
        self.account = config['PAPER_ACCOUNT']
        super(IBWebAccountHandler, self).__init__(self.events, self.port, self.client_id)
        self._initialize_handlers()
        self.curr_dt = dt.datetime.now()

        self._req_account_updates()
        self.account_info = {
            'cash_balance':     0, #CashBalance
            'available_funds':  0, #AvailableFunds
            'buying_power':     0, #BuyingPower
            'excess_liquidity': 0, #ExcessLiquidity
            'margin_maint_req': 0, # FullMaintMarginReq
            'futures_pnl':      0,  #FuturesPNL
            'net_liquidation':  0,
            'unrealized_pnl':   0, #UnrealizedPnL
            'realized_pnl':     0, #RealizedPnL
        }

        self.portfolio = {}

        # Reply handler thread
        thread = threading.Thread(target=self._reply_handler, args=())
        thread.daemon = True
        thread.start()

        log.info("IBDataHandler initialized!")

    def _initialize_handlers(self):
        self.reply_handlers = {
            'error': super(IBWebAccountHandler, self).handle_error_msg,
            'connectionClosed': super(IBWebAccountHandler, self).handle_connection_closed_msg,
            'managedAccounts': super(IBWebAccountHandler, self).handle_managed_accounts_msg,
            'nextValidId': super(IBWebAccountHandler, self).handle_next_valid_id_msg,
            'updateAccountValue': self._handle_update_account_value_msg,
            'updatePortfolio': self._handle_update_portfolio_msg,
        }
        self._init_account_fields()
        self._init_portfolio_fields()

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

    def _init_account_fields(self):
        self.account_fields = {
            'CashBalance':          'cash_balance',
            'AvailableFunds':       'available_funds',
            'BuyingPower':          'buying_power',
            'ExcessLiquidity':      'excess_liquidity',
            'FullMaintMarginReq':   'margin_maint_req',
            'FuturesPNL':           'futures_pnl',
            'UnrealizedPnL':        'unrealized_pnl',
            'RealizedPnL':          'realized_pnl',
        }

    def _init_portfolio_fields(self):
        self.portfolio_fields = {
            'marketValue':      'market_value',
            'realizedPNL':      'realized_pnl',
            'unrealizedPNL':    'unrealized_pnl',
            'marketPrice':      'market_price',
            'averageCost':      'average_cost',
            'position':         'position',
        }

    def _req_account_updates(self):
        self.connection.reqAccountUpdates(True, self.account)

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
                except KeyError as e:
                    # print "{} Need to handle message type: {}".format(repr(e), msg.typeName)
                    pass
            except IndexError:
                time.sleep(self.msg_interval)

    def _handle_update_account_value_msg(self, msg):
        account_field = self.account_fields[msg['key']]
        self.account_info[account_field] = msg['value']

    def _handle_update_portfolio_msg(self, msg):
        ticker = get_contract_ticker(msg['contract'])
        if ticker not in self.portfolio:
            self.portfolio[ticker] = {}
        for msg_key, portfolio_key in self.portfolio_fields.items():
            self.portfolio[ticker][portfolio_key] = msg[msg_key]