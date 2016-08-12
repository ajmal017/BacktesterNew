import sys
import os
import logging
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s')
log = logging.getLogger('Strategy')
from queue import Queue
import pandas as pd
import datetime as dt
import numpy as np
import prediction.features as feats
from trading.futures_contract import FuturesContract
from cme_backtest.data_handler import CMEBacktestDataHandler
from trading.futures_strategy import FuturesStrategy
from cme_backtest.execution_handler import CMEBacktestExecutionHandler
from cme_backtest.backtest import CMEBacktest

NOT_UPDATING_FEATURES = False
BACKTEST_NAME = None
RUN_TIME = dt.datetime.now()

granularity = 5
hl = int(3840 / granularity)
window = int(7680 / granularity)

window_td = pd.Timedelta(seconds=int(7860/granularity))

x_feats = [
    feats.mean_reversion_signal(hl, window),
    feats.ema_diff(window/2)
]

class ClassifierStrategy(FuturesStrategy):

    def initialize(self, contract_multiplier={}, transaction_costs={}, slippage=0, starting_cash=100000,
                   min_hold_time=dt.timedelta(minutes=15), max_hold_time=dt.timedelta(hours=2), start_date=None, end_date=None,
                   start_time=dt.time(hour=0), closing_time=dt.time(hour=23, minute=59), standardize=False, take_profit_threshold=None,
                   take_profit_down_only=False, order_qty=1, *args, **kwargs):

        self.symbols = [product.symbol for product in self.products]
        self.symbol = self.symbols[0]
        self.product = self.products[0]
        self.contract_multiplier = contract_multiplier
        self.transaction_costs = transaction_costs
        self.slippage = slippage
        self.starting_cash = starting_cash
        self.min_hold_time = min_hold_time
        self.max_hold_time = max_hold_time
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = start_time
        self.closing_time = closing_time
        self.standardize = standardize
        self.take_profit_threshold = take_profit_threshold
        self.take_profit_down_only = take_profit_down_only
        self.order_qty = order_qty
        self.retrain_period = 0
        self.last_order_time = {sym: None for sym in self.symbols}
        self.pos = {sym: 0 for sym in self.symbols}
        self.implied_pos = {sym: 0 for sym in self.symbols}
        self.entry_price = {sym: None for sym in self.symbols}
        self.cash = self.starting_cash
        self.pnl = []
        self.daily_pnl = [self.starting_cash]
        # self.price_series = {sym: [] for sym in self.symbols}
        self.spread = {sym: [] for sym in self.symbols}
        # self.time_series = []
        self.orders = {sym: [] for sym in self.symbols}
        self.kill_till = {sym: None for sym in self.symbols}
        self.total_signals = {sym: [] for sym in self.symbols}
        self.total_probs = {sym: [] for sym in self.symbols}
        self.total_pnl = []
        self.total_price_series = {sym: [] for sym in self.symbols}
        self.total_spread = {sym: [] for sym in self.symbols}
        self.total_time_series = []
        self.total_orders = {sym: [] for sym in self.symbols}
        self.kill_till = {sym: None for sym in self.symbols}
        self.signals = {sym: [] for sym in self.symbols}
        self.probs = {sym: [] for sym in self.symbols}
        self.true_probs = {sym: [] for sym in self.symbols}

    def add_features(self, bars):
        for feat in x_feats:
            feat(bars)
        bars.fillna(0, inplace=True)

    def new_tick(self):
        bars = self.get_latest_bars(self.symbol, window=window_td)
        self.add_features(bars)
        bar = bars.iloc[-1]
        for sym in self.symbols:
            try:
                pos = self.implied_pos[sym]
                prob = bar['mean_reversion_signal_768_1536']
                ema_diff = bar['ema_diff_768']
                signal = int(-prob)
                if abs(signal) > 3:
                    signal = 3 * np.sign(signal)

                # close out
                if self.curr_dt.time() >= self.closing_time and pos != 0:
                    self.order(self.product, -pos)
                    self.implied_pos[sym] += -pos
                    self.last_order_time[sym] = self.curr_dt

                elif self.curr_dt.time() >= self.start_time and self.curr_dt.time() < self.closing_time:
                    if self.curr_dt.time() >= self.closing_time and pos == 0:
                        pass
                    # increase position
                    elif self.order_qty*abs(signal) > abs(pos):
                        self.order(self.product, signal*self.order_qty - pos)
                        self.implied_pos[sym] += signal*self.order_qty - pos
                        if np.sign(pos) != np.sign(signal):
                            self.last_order_time[sym] = self.curr_dt

                    elif pos != 0 and np.sign(ema_diff) != -np.sign(pos):
                        self.order(self.product, -pos)
                        self.implied_pos[sym] += -pos
                        self.last_order_time[sym] = None

                    elif pos != 0:
                        self.check_stop_loss(sym)

                self.signals[sym].append(0)
                self.probs[sym].append(prob)
                # self.positions[sym].append(pos)

            except Exception as e:
                # self.signals[sym].append(0)
                # self.probs[sym].append(0)
                # self.positions[sym].append(0)
                repr(e)

        # if len(self.signals[self.symbols[0]]) != len(self.time_series):
        #     print 'signal len', len(self.signals[self.symbols[0]])
        #     print 'ts len', len(self.time_series)
        #     raise Exception("FUCK YOU")

    def new_day(self, newday_event):
        pass
        # print "new day", newday_event.next_date
        #
        # if len(self.time_series) == 0:
        #     return
        #
        # self.daily_pnl.append(self.pnl[-1])
        #
        # self.total_time_series += self.time_series
        # self.total_pnl += self.pnl
        # for sym in self.symbols:
        #     self.total_price_series[sym] += self.price_series[sym]
        #     self.total_orders[sym] += self.orders[sym]
        #     self.total_signals[sym] += self.signals[sym]
        #     self.total_probs[sym] += self.probs[sym]
        #
        #
        # # self.time_series = []
        # self.pnl = []
        # for sym in self.symbols:
        #     self.price_series[sym] = []
        #     self.orders[sym] = []
        #     self.signals[sym] = []
        #     self.probs[sym] = []
        #     self.true_probs[sym] = []

    def new_fill(self, fill_event):
        sym = fill_event.symbol
        if self.pos[sym] == 0:
            self.entry_price[sym] = None
        else:
            self.entry_price[sym] = fill_event.fill_cost / float(fill_event.quantity)

    def update_metrics(self):
        pass
        # last_bar = self.get_latest_bars(self.symbol, n=1)
        # pnl_ = self.cash + sum([self.pos[sym] * self.contract_multiplier[sym] *
        #                         (last_bar['level_1_price_buy']
        #                          if self.pos[sym] < 0 else
        #                          last_bar['level_1_price_sell']) for sym in self.symbols])
        # self.pnl.append(pnl_)
        # self.time_series.append(self.curr_dt)
        # for sym in self.symbols:
        #     self.price_series[sym].append((last_bar['level_1_price_buy'] + last_bar['level_1_price_sell']) / 2.)
        #     self.spread[sym].append(last_bar['level_1_price_sell'] - last_bar['level_1_price_buy'])

    def check_stop_loss(self, sym):
        pos = self.implied_pos[sym]
        # TODO - improve stop-loss handling
        """
        if (pos != 0 and len(self.pnl) > 600 and self.pnl[-600] - self.pnl[-1] > 250) and \
                (self.kill_till[sym] is None or self.kill_till[sym] <= self.curr_dt):
            self.order(sym, -pos)
            self.order(sym, -pos)
            self.implied_pos[sym] += -2*pos
            self.kill_till[sym] = self.curr_dt + dt.timedelta(minutes=15)
            self.last_order_time[sym] = self.curr_dt
        """

    def _build_forwardtest_fpath(self, fname):
        pass

def run_forwardtest():
    start_time = dt.time(hour=3)
    closing_time = dt.time(hour=20)
    standardize = False
    take_profit_threshold = None
    start_date = dt.datetime(year=2015, month=11, day=1)
    end_date = dt.datetime(year=2015, month=11, day=30)
    products = [FuturesContract('GC', exp_year=2016, exp_month=6)]
    symbols = ['GCZ5']
    start_date = dt.datetime.strptime(sys.argv[2], "%Y-%m-%d")
    end_date = dt.datetime.strptime(sys.argv[3], "%Y-%m-%d")
    symbols = [sys.argv[1]]
    contract_multiplier = {
        symbols[0]: 1000
    }
    transaction_costs = {
        symbols[0]: 1.45
    }

    events = Queue()
    bars = CMEBacktestDataHandler(events, symbols, start_date, end_date,
                                    second_bars=True,
                                    start_time=dt.timedelta(hours=3),
                                    end_time=dt.timedelta(hours=22))
    strategy = ClassifierStrategy(events, bars, products, 100000,
                                  load_classifier=True,
                                  contract_multiplier=contract_multiplier,
                                  transaction_costs=transaction_costs,
                                  slippage=0.01,
                                  min_hold_time=dt.timedelta(minutes=5),
                                  max_hold_time=dt.timedelta(hours=12),
                                  start_date=start_date,
                                  end_date=end_date,
                                  start_time=start_time,
                                  closing_time=closing_time,
                                  standardize=standardize)
    execution = CMEBacktestExecutionHandler(events, symbols, second_bars=True)
    backtest = CMEBacktest(events, bars, strategy, execution, start_date, end_date)
    backtest.run()

if __name__ == "__main__":
    run_forwardtest()