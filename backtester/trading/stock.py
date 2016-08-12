import datetime as dt
import utils.yahoo_finance as yf
from dateutil.tz import tzlocal
from interactive_brokers.ib_utils import create_ib_stock_contract

MARKET_OPEN = dt.datetime(year=2016, month=1, day=1, hour=8, minute=30, tzinfo=tzlocal()).time()
MARKET_CLOSE = dt.datetime(year=2016, month=1, day=1, hour=3, minute=0, tzinfo=tzlocal()).time()


class Stock(object):
    def __init__(self, symbol, name=None, sector=None, ib=False):
        self.symbol = symbol
        self.name = name if name is not None else yf.get_company_name(symbol)
        self.sector = sector if sector is not None else yf.get_company_sector(symbol)

        self.mkt_open = MARKET_OPEN
        self.mkt_close = MARKET_CLOSE

        self.ib_contract = create_ib_stock_contract(self.symbol)
