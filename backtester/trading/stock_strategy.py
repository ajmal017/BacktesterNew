import numpy as np
import pandas as pd

from trading.strategy import Strategy


class StockStrategy(Strategy):
    def __init__(self, events, data, products, initial_cash=1000000, price_field='Open'):
        super(StockStrategy, self).__init__(events, data, products, initial_cash)
        self.price_field = price_field
        mkt_price_columns = [product.symbol+'_mkt' for product in self.products]
        position_columns = [product.symbol+'_pos' for product in self.products]
        columns = ['dt'] + mkt_price_columns + position_columns + ['cash']
        self.time_series = pd.DataFrame(data=None, columns=columns)

    def new_tick_update(self, market_event):
        self.curr_dt = market_event.dt
        self.last_bar = self.data.last_bar.copy()
        _mkt_prices = [self.last_bar[product.symbol][self.price_field] for product in self.products]
        _positions = [self.positions[product.symbol] for product in self.products]
        self.time_series.loc[len(self.time_series)] = [self.curr_dt] + _mkt_prices + _positions + [self.cash]

    def finished(self, save=False):
        for product in self.products:
            self.time_series[product.symbol] = self.time_series[product.symbol+'_pos']*\
                                               self.time_series[product.symbol+'_mkt']

        self.time_series['total_val'] = np.sum(self.time_series[product.symbol] for product in self.products) \
                                        + self.time_series['cash']
        self.time_series.set_index('dt', inplace=True)
        self.transactions_series.set_index('dt', inplace=True)
        self.returns_series = self.time_series['total_val'].pct_change().fillna(0)
        positions_cols = [product.symbol for product in self.products] + ['cash']
        self.positions_series = pd.DataFrame(data=np.array([self.time_series[product.symbol]
                                                            for product in self.products]
                                                           +[self.time_series['cash']]).transpose(),
                                             columns=positions_cols,
                                             index=self.time_series.index)