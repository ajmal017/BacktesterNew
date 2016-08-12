import numpy as np
import pandas as pd
import datetime as dt
from trading.strategy import Strategy
from cme_backtest.data_utils.quantgo_utils import _reindex_data
from futures_utils import get_base_symbol_from_symbol

BUFFER_SIZE = 100000

class FuturesStrategy(Strategy):

    def __init__(self, events, data, products, initial_cash=0, continuous=True, live=False):
        super(FuturesStrategy, self).__init__(events, data, products, initial_cash)

        # map ticker:product
        self.ticker_prods = {product.symbol: product for product in self.products}

        self.pnl_series = []

        self.transactions_series = []  #(fill_time, quantity, fill_price, ticker)
        # self.cash_series = pd.Series(data=None)
        # self.positions_series = {product.symbol: pd.Series(data=None) for product in self.products}
        # self.transactions_series = {product.symbol: pd.DataFrame(data=None, columns=['dt', 'amount', 'price', 'symbol']) for product in self.products}
        # self.time_series = self._make_time_series_df(0, columns=['dt', 'level_1_price_buy', 'level_1_price_sell'])
        # self.time_series_index = 0
        self.live = live

    def _make_time_series_df(self, start_index, columns):
        """
        Creates an empty Multi-Index DataFrame to store time-series.
        :return:
        """
        d = {product.symbol: pd.DataFrame(data=None,
                                          columns=columns,
                                          index=np.arange(start_index, start_index+BUFFER_SIZE, step=1))
             for product in self.products}
        reform = {(k_outer, k_inner): values for k_outer, d_inner in d.iteritems()
                  for k_inner, values in d_inner.iteritems()}
        df = pd.DataFrame(reform)
        return df

    def _new_tick_update_backtest(self, market_event):
        """
        Take the data directly from the data handler (for historical data)
        :param market_event:
        :return:
        """


    def _new_tick_update_live(self, market_event):
        """
        Stream the data into a buffer.
        :param market_event:
        :return:
        """
        pass

    def new_tick_update(self, market_event):
        """
            Update:
            - pnl series
            - price_series (last_bar)
            - returns (series of decimal of cumulative PnL)
            - transactions (basically the fills)
            - positions series
        :param market_event:
        :return:
        """
        self.curr_dt = market_event.dt
        if self.live:
            self._new_tick_update_live(market_event)
        else:
            self._new_tick_update_backtest(market_event)
        # self.curr_dt = market_event.dt
        self.last_bar = market_event.data
        # self.time_series.iloc[self.time_series_index] = self.last_bar  # set the data
        # self.time_series_index += 1
        # if self.time_series_index == len(self.time_series):
        #     empty_df = self._make_time_series_df(self.time_series_index,
        #                                          columns=['dt', 'level_1_price_buy', 'level_1_price_sell'])
        #     self.time_series = pd.DataFrame(pd.concat([self.time_series, empty_df], copy=False))

    def new_fill_update(self, fill_event):
        self.positions[fill_event.symbol] += fill_event.quantity
        self.cash -= fill_event.fill_cost
        #self.ticker_prods[get_base_symbol_from_symbol(fill_event.symbol)] * fill_event.fill_cost
        transaction = (fill_event.fill_time, fill_event.quantity, fill_event.fill_price, fill_event.symbol)
        self.transactions_series.append(transaction)

    def get_latest_bars(self, symbol, window, start=None):
        """
        Get data for a symbol from [start-window, start]
        :param symbol:
        :param window: (pd.Timedelta)
        :param start: (pd.Timestamp / DateTime) defaults to self.curr_dt
        :return:
        """
        start = start if start is not None else self.curr_dt
        if self.live is False:
            return self._get_latest_bars_backtest(symbol, window, start)
        else:
            return self._get_latest_bars_live()

    def _get_latest_bars_backtest(self, symbol, window, start):
        before = start - window
        return self.data.curr_day_data[symbol].truncate(before, start)

    def _get_latest_bars_live(self):
        pass


    def finished(self):
        pass
        # for symbol_time_series in self.time_series.values():
        #     symbol_time_series.set_index('dt', inplace=True)

# self.time_series.ix[self.time_series_index] = self.curr_dt    # set the dt index
# self.time_series.append(market_event.data, ignore_index=True)
# display(self.time_series.iat[len(self.time_series):])
# self.time_series.iat[len(self.time_series):] = market_event.data
# print self.time_series.tail(1)
# self.last_bar = self.data.last_bar.copy()
# for product in self.products:
#     mkt_bid = self.last_bar[product.symbol]['level_1_price_buy']
#     mkt_ask = self.last_bar[product.symbol]['level_1_price_sell']
#     pos = self.positions[product.symbol]
#     self.time_series[product.symbol].loc[len(self.time_series[product.symbol])] = \
#         [self.curr_dt, mkt_bid, mkt_ask, pos, self.cash] + \
#         [0]*(len(self.time_series[product.symbol].keys())-5)

# add placeholder for any features that need to be calculates
# for product in self.products:
#     # self.time_series[product.symbol+'_mkt'].append((last_bar['level_1_price_buy'] + last_bar['level_1_price_sell']) / 2.)
#     mkt_price = (self.last_bar[product.symbol]['level_1_price_buy'] + self.last_bar[product.symbol]['level_1_price_sell'])/2.
#     pnl_ = self.cash + sum([self.positions[product.symbol] * product.tick_value * (self.last_bar[product.symbol]['level_1_price_buy']
#                             if self.positions[product.symbol] < 0
#                             else self.last_bar['level_1_price_sell']) for product.symbol in self.products])
#     # self.spread[sym].append(last_bar['level_1_price_sell'] - last_bar['level_1_price_buy'])
# def update_metrics(self):
#     last_bar = self.get_latest_bars(self.symbol, n=1)
#     pnl_ = self.cash + sum([self.pos[sym] * self.contract_multiplier[sym] *
#                             (last_bar['level_1_price_buy']
#                              if self.pos[sym] < 0 else
#                              last_bar['level_1_price_sell']) for sym in self.symbols])
#     self.pnl.append(pnl_)
#     self.time_series.append(self.cur_time)
#     for sym in self.symbols:
#         self.price_series[sym].append((last_bar['level_1_price_buy'] + last_bar['level_1_price_sell']) / 2.)
#         self.spread[sym].append(last_bar['level_1_price_sell'] - last_bar['level_1_price_buy'])
