from __future__ import division

import os
import warnings
import numpy as np
import pandas as pd
from prediction.featutils import add_log_returns, standardize_features, add_mid_price
from cme_backtest.data_utils.data_path import get_file_path, get_date_and_sym
import statsmodels.api as sm

"""
features TODO

try 10 second, 30 second, 1 minute, 2 minute lookahead
-log_returns_Nw+ - N is MA lookahead window
-***log_returns_NwL2+ - N is MA lookahead window, L2 average

try 10 second, 30 second, 1 minute, 2 minute lookback
-log_returns_Nw- - N is MA lookahead window
-***log_returns_NwL2- - N is MA lookahead window, L2 average

-first derivatives of log_returns_Nw- or log_returns_NwL2-

try 10 seconds, 30 seconds, 1 minute, 2 minute lookback
-realized_volatility_N-

-volume imbalance

-spread size

-VPIN (requires trade data)

-fill likelihood (not possible without making some model)

-features from related products

-***presence of large order

TODO - correct for corruption of values when midpoint price changes from levels being revealed/hidden

build some basic workflow tools after analyzing these features

"""


def load_features(symbol, date, subscription, save_ext):
    fpath = get_file_path(symbol, date, subscription, extension=save_ext)
    data = pd.read_csv(fpath, parse_dates=[0])
    return data


def get_feature_names(feats, sample_data):
    # TODO - how many times does this get called? 5 times?
    sample_data = sample_data.reset_index()
    sample_data['symbol'] = 'feat_name'
    return sum([feat(sample_data) for feat in feats], [])


def add_features(datas, standardize=True, inplace=False,
                 xfeats_post_filter=False, yfeats_post_filter=False, save_ext='', start_time=None, end_time=None,
                 load_if_exists=True, subscription='CME_Level_2', x_feats=[], y_feats=[], filters=[], drop_cols=[]):

    if type(datas) != list:
        datas = [datas]

    output_data = []

    for i, data in enumerate(datas):
        date, sym = get_date_and_sym(data)
        fpath = get_file_path(sym, date, subscription, extension=save_ext)
        print fpath
        meta_path = get_file_path(sym, date, subscription, extension=save_ext+"_meta")

        if load_if_exists and os.path.exists(fpath) and os.path.exists(meta_path):
            if inplace:
                warnings.warn("Inplace modification of data not possible when loading features", UserWarning)
            data = pd.read_csv(fpath, parse_dates=[0])
            with open(meta_path, 'r') as f:
                xfeatures, yfeatures = map(lambda l: l.replace("\n", "").split(","), f.readlines())
        else:
            if not inplace:
                data = data.copy(deep=True)
            if not xfeats_post_filter:
                xfeatures = sum([feat(data) for feat in x_feats], [])
            if not yfeats_post_filter:
                yfeatures = sum([feat(data) for feat in y_feats], [])
            for filt in filters:
                data.drop(data[~filt(data)].index, axis=0, inplace=True)
            if start_time is not None:
                data = data[(data.index >= data.index[0] + start_time)]
            if end_time is not None:
                data = data[(data.index <= data.index[0] + end_time - start_time)]
            if xfeats_post_filter:
                xfeatures = sum([feat(data) for feat in x_feats], [])
            if yfeats_post_filter:
                yfeatures = sum([feat(data) for feat in y_feats], [])
            data.drop(drop_cols, axis=1, inplace=True)
            data.fillna(0, inplace=True)
            if save_ext:
                date, sym = get_date_and_sym(data)
                fpath = get_file_path(sym, date, subscription, extension=save_ext)
                data.to_csv(fpath)
                with open(meta_path, 'w') as f:
                    f.write(",".join(xfeatures) + "\n")
                    f.write(",".join(yfeatures) + "\n")

        output_data.append(data)

    output_data = pd.concat(output_data)
    output_data = output_data.reset_index().set_index('time')

    if standardize:
        standardize_features(data, xfeatures)

    print "done adding features..."

    return output_data, xfeatures, yfeatures


def feature(func):
    def wrapped_func(*args, **kwargs):
        return lambda data: func(data, *args, **kwargs)
    return wrapped_func


@feature
def spread_size(data, window, do_raise=True):
    data['spread_size_{}w'.format(window)] = pd.rolling_mean(data['level_1_price_sell'] - data['level_1_price_buy'], window=window, min_periods=0)
    return ['spread_size_{}w'.format(window)]


@feature
def future_log_returns(data, window, do_raise=True):
    if 'log_returns' not in data.columns:
        add_log_returns(data)
    data['log_returns_{}w+'.format(window)] = np.concatenate([(pd.rolling_mean(data['log_returns'].values[::-1], window=window, min_periods=0))[:0:-1], [0]])
    return ['log_returns_{}w+'.format(window)]


@feature
def future_log_returns_l2(data, window, do_raise=True):
    if 'log_returns' not in data.columns:
        add_log_returns(data)
    l2 = lambda x: np.sign(x) * x**2
    ssqrt = lambda x: np.sign(x) * np.sqrt(np.abs(x))
    data['log_returns_l2_{}w+'.format(window)] = ssqrt(np.concatenate([pd.rolling_mean(l2(data['log_returns'].values[::-1]), window=window, min_periods=0)[:0:-1], [0]]))
    return ['log_returns_l2_{}w+'.format(window)]


@feature
def future_abs_log_returns_l2(data, window, do_raise=True):
    if 'log_returns' not in data.columns:
        add_log_returns(data)
    l2 = lambda x: x**2
    data['abs_log_returns_l2_{}w+'.format(window)] = np.sqrt(np.concatenate([pd.rolling_mean(l2(data['log_returns'].values[::-1]), window=window, min_periods=0)[:0:-1], [0]]))
    return ['abs_log_returns_l2_{}w+'.format(window)]


@feature
def future_profit(data, window, do_raise=True):
    if 'mid_price' not in data.columns:
        add_mid_price(data)
    dprice = np.concatenate([[0], np.diff(data['mid_price'].values)])
    data['profit_{}w+'.format(window)] = np.concatenate([pd.rolling_mean(dprice[::-1], window=window, min_periods=0)[:0:-1], [0]])
    return ['profit_{}w+'.format(window)]


@feature
def future_profit_std(data, window, do_raise=True):
    if 'mid_price' not in data.columns:
        add_mid_price(data)
    dprice = np.concatenate([[0], np.diff(data['mid_price'].values)])
    data['profit_std_{}w+'.format(window)] = np.concatenate([pd.rolling_std(dprice[::-1], window=window, min_periods=0)[:0:-1], [0]])
    return ['profit_std_{}w+'.format(window)]


@feature
def future_score(data, window, do_raise=True):
    fprofit_col = 'profit_{}w+'.format(window)
    fprofit_std_col = 'profit_std_{}w+'.format(window)
    if fprofit_col not in data.columns:
        future_profit(window)(data)
    if fprofit_std_col not in data.columns:
        future_profit_std(window)(data)
    featname = 'score_{}w+'.format(window)
    data[featname] = data[fprofit_col] / (0.1 + data[fprofit_std_col])
    return [featname]



@feature
def future_pnl(data, window, spread_penalty=1.0, do_raise=True):
    l2 = lambda x: np.sign(x) * x**2
    ssqrt = lambda x: np.sign(x) * np.sqrt(np.abs(x))
    buy_col = 'future_pnl_l2_buy_{}w+_{}sp'.format(window, spread_penalty)
    sell_col = 'future_pnl_l2_sell_{}w+_{}sp'.format(window, spread_penalty)
    data[buy_col] = 0
    data[sell_col] = 0
    for w in xrange(1, window+1):
        buy_pnl = (1-spread_penalty) * data['level_1_price_sell'].values[w:] + spread_penalty * data['level_1_price_buy'].values[w:] - \
                  (1-spread_penalty) * data['level_1_price_buy'].values[:-w] - spread_penalty * data['level_1_price_sell'].values[:-w]
        sell_pnl = (1-spread_penalty) * data['level_1_price_sell'].values[:-w] + spread_penalty * data['level_1_price_buy'].values[:-w] - \
                   (1-spread_penalty) * data['level_1_price_buy'].values[w:] - spread_penalty * data['level_1_price_sell'].values[w:]
        data[buy_col] += np.concatenate([l2(buy_pnl), [0]*w])
        data[sell_col] += np.concatenate([l2(sell_pnl), [0]*w])
    data[buy_col] = ssqrt(data[buy_col] / window)
    data[sell_col] = ssqrt(data[sell_col] / window)
    return [buy_col, sell_col]


@feature
def log_returns_ma(data, window, do_raise=True):
    if 'log_returns' not in data.columns:
        add_log_returns(data)
    data['log_returns_{}w-'.format(window)] = pd.rolling_mean(data['log_returns'], window=window, min_periods=0)
    return ['log_returns_{}w-'.format(window)]


@feature
def dma(data, window, do_raise=True):
    if 'mid_price' not in data.columns:
        add_mid_price(data)
    colname = 'dma_{}'.format(window)
    ma = pd.rolling_mean(data['mid_price'], window=window, min_periods=0)
    data[colname] = np.concatenate([[0], np.diff(ma)])
    return [colname]


@feature
def dema(data, window, do_raise=True):
    if 'mid_price' not in data.columns:
        add_mid_price(data)
    colname = 'dema_{}'.format(window)
    ma = pd.ewma(data['mid_price'], halflife=window)
    data[colname] = np.concatenate([[0], np.diff(ma)])
    return [colname]


@feature
def dma_ddma_prod(data, window):
    data['dmaXddma_{}'.format(window)] = data['dma_{}'.format(window)] * data['d_dma_{}'.format(window)]
    return ['dmaXddma_{}'.format(window)]


@feature
def dma_diffs(data, windows, do_raise=True):
    names = []
    if 'mid_price' not in data.columns:
        add_mid_price(data)
    for window in windows:
        ma = pd.rolling_mean(data['mid_price'], window=window, min_periods=0)
        data['dma_{}'.format(window)] = np.concatenate([[0], np.diff(ma)])
    for w1 in windows:
        for w2 in windows:
            if w1 > w2:
                feat_name = 'dma_diff_{}-{}'.format(w1, w2)
                data[feat_name] = data['dma_{}'.format(w1)] - data['dma_{}'.format(w2)]
                names.append(feat_name)
    return names


@feature
def ma_diffs(data, windows, do_raise=True):
    feat_names = []
    for window in windows:
        data['ma_{}'.format(window)] = pd.rolling_mean(data['mid_price'], window=window, min_periods=0)
    for w1 in windows:
        for w2 in windows:
            if w1 > w2:
                feat_name = 'ma_diff_{}-{}'.format(w1, w2)
                data[feat_name] = data['ma_{}'.format(w1)] - data['ma_{}'.format(w2)]
                feat_names.append(feat_name)
    return feat_names


@feature
def dema_diffs(data, windows, do_raise=True):
    names = []
    if 'mid_price' not in data.columns:
        add_mid_price(data)
    for window in windows:
        ma = pd.ewma(data['mid_price'], halflife=window)
        data['dema_{}'.format(window)] = np.concatenate([[0], np.diff(ma)])
    for w1 in windows:
        for w2 in windows:
            if w1 > w2:
                feat_name = 'dema_diff_{}-{}'.format(w1, w2)
                data[feat_name] = data['dema_{}'.format(w1)] - data['dema_{}'.format(w2)]
                names.append(feat_name)
    return names


@feature
def ema_diffs(data, windows, do_raise=True):
    feat_names = []
    for window in windows:
        data['ema_{}'.format(window)] = pd.ewma(data['mid_price'], halflife=window)
    for w1 in windows:
        for w2 in windows:
            if w1 > w2:
                feat_name = 'ema_diff_{}-{}'.format(w1, w2)
                data[feat_name] = data['ema_{}'.format(w1)] - data['ema_{}'.format(w2)]
                feat_names.append(feat_name)
    return feat_names


@feature
def ma_dma_diff_prod(data, windows):
    feat_names = []
    for w1 in windows:
        for w2 in windows:
            if w1 > w2:
                feat_name = 'maXdma_diff_{}-{}'.format(w1, w2)
                data[feat_name] = data['ma_diff_{}-{}'.format(w1, w2)] * data['dma_diff_{}-{}'.format(w1, w2)]
                feat_names.append(feat_name)
    return feat_names


@feature
def derivative(data, feat_name, do_raise=True):
    data['d_{}'.format(feat_name)] = np.concatenate([[0], np.diff(data[feat_name])])
    return ['d_{}'.format(feat_name)]


@feature
def square(data, feat_name, do_raise=True):
    data['{}^2'.format(feat_name)] = data[feat_name] ** 2
    return ['{}^2'.format(feat_name)]


@feature
def ma(data, feat_name, window, do_raise=True):
    data['{}_{}w'.format(feat_name, window)] = pd.rolling_mean(data[feat_name], window=window, min_periods=0)
    return ['{}_{}w'.format(feat_name, window)]


@feature
def ema(data, feat_name, window, do_raise=True):
    data['{}_{}_ema'.format(feat_name, window)] = pd.ewma(data[feat_name], halflife=window)
    return ['{}_{}_ema'.format(feat_name, window)]


@feature
def d_bid(data, window, do_raise=True):
    data['d_bid_{}w'.format(window)] = pd.rolling_mean(np.concatenate([[0], np.diff(data['level_1_price_buy'])]), window=window, min_periods=0)
    return ['d_bid_{}w'.format(window)]


@feature
def d_ask(data, window, do_raise=True):
    data['d_ask_{}w'.format(window)] = pd.rolling_mean(np.concatenate([[0], np.diff(data['level_1_price_sell'])]), window=window, min_periods=0)
    return ['d_ask_{}w'.format(window)]


@feature
def book_diff(data, depth=5, do_raise=True):
    bids = sum([data['level_{}_volume_buy'.format(i)] for i in range(1, depth+1)])
    offers = sum([data['level_{}_volume_sell'.format(i)] for i in range(1, depth+1)])
    data['book_diff'] = bids - offers
    return ['book_diff']


@feature
def bid_vpo(data, max_level=5, do_raise=True):
    data['bid_vpo'] = sum([(data['level_{}_volume_buy'.format(i)] / data['level_{}_orders_buy'.format(i)].apply(lambda v: max(1,v))).fillna(0) for i in range(1, max_level+1)])
    return ['bid_vpo']



@feature
def ask_vpo(data, max_level=5, do_raise=True):
    data['ask_vpo'] = sum([(data['level_{}_volume_sell'.format(i)] / data['level_{}_orders_sell'.format(i)].apply(lambda v: max(1,v))).fillna(0) for i in range(1, max_level+1)])
    return ['ask_vpo']


@feature
def bbo_change(data, do_raise=True):
    try:
        data['bid_change'] = 0
        data.ix[np.concatenate([[False], data['level_1_price_buy'].values[:-1] < data['level_1_price_buy'].values[1:]]), 'bid_change'] = 1
        data.ix[np.concatenate([[False], data['level_1_price_buy'].values[:-1] > data['level_1_price_buy'].values[1:]]), 'bid_change'] = -1

        data['ask_change'] = 0
        data.ix[np.concatenate([[False], data['level_1_price_sell'].values[:-1] < data['level_1_price_sell'].values[1:]]), 'ask_change'] = 1
        data.ix[np.concatenate([[False], data['level_1_price_sell'].values[:-1] > data['level_1_price_sell'].values[1:]]), 'ask_change'] = -1
    except Exception as e:
        if data != 'feat_name':
            raise e
    return ['bid_change', 'ask_change']


@feature
def book_diff_change(data, do_raise=True):
    try:
        if 'book_diff' not in data.columns:
            book_diff()(data)
        if 'bbo_change' not in data.columns:
            bbo_change()(data)
        data['book_diff_change'] = np.concatenate([[0], np.diff(data['book_diff'])])
        bbo_changed = (data['bid_change'] != 0) | (data['ask_change'] != 0)
        mean_from_bbo_change = data.ix[bbo_changed, 'book_diff_change'].mean()
        data.ix[bbo_changed, 'book_diff_change'] -= mean_from_bbo_change
    except Exception as e:
        if data != 'feat_name':
            raise e
    return ['book_diff_change']


@feature
def new_orders(data, depth=5, do_raise=True):
    try:
        if 'bbo_change' not in data.columns:
            bbo_change()(data)
        ask_qty = sum([data['level_{}_volume_sell'.format(i)] for i in xrange(1, depth+1)])
        bid_qty = sum([data['level_{}_volume_buy'.format(i)] for i in xrange(1, depth+1)])

        [('level_{}_price_buy'.format(i), 'level_{}_volume_buy'.format(i), 'level_{}_depth_buy'.format(i)) for i in xrange(1, depth+1)]
        data['bid_qty_change_{}'.format(depth)] = np.concatenate([[0], np.diff(ask_qty)])
        data['ask_qty_change_{}'.format(depth)] = np.concatenate([[0], np.diff(bid_qty)])
    except Exception as e:
        if data != 'feat_name':
            raise e
    return ['bid_qty_change_{}'.format(depth), 'ask_qty_change_{}'.format(depth)]


@feature
def rsi(data, size, window, do_raise=True):
    try:
        p = data['mid_price'].values
        dp = p[size:] - p[:-size]
        dUp, dDown = dp.copy(), dp.copy()
        dUp[dUp < 0] = 0
        dDown[dDown > 0] = 0
        rollUp = pd.rolling_mean(dUp, window, min_periods=0)
        rollDown = -pd.rolling_mean(dDown, window, min_periods=0)
        rs = rollUp / rollDown
        rsi = 100 - 100 / (1 + rs)
        featname = 'rsi_{}s_{}w'.format(size, window)
        #data[featname] = np.concatenate([[50]*size, rsi])
        data[featname] = np.concatenate([[50]*min(size, len(data) - len(rsi)), rsi])
    except Exception as e:
        if data.reset_index()['symbol'].values[0] != 'feat_name':
            raise e
    return [featname]


@feature
def mean_reversion_signal(data, hl, window):
    if 'mid_price' not in data.columns:
        add_mid_price(data)
    ema = pd.ewma(data['mid_price'], halflife=hl)
    std = pd.rolling_std(data['mid_price'], window=window, min_periods=0)
    data['mean_reversion_signal_{}_{}'.format(hl, window)] = (data['mid_price'] - ema) / std
    return ['mean_reversion_signal_{}_{}'.format(hl, window)]


@feature
def ema_diff(data, hl):
    if 'mid_price' not in data.columns:
        add_mid_price(data)
    ema = pd.ewma(data['mid_price'], halflife=hl)
    data['ema_diff_{}'.format(hl)] = data['mid_price'] - ema
    return ['ema_diff_{}'.format(hl)]


@feature
def hurst_exp(data, window, step):
    data['hurst_exp_{}_{}'.format(window, step)] = 0
    for i in xrange(0, len(data)-window-step, step):
        y = data.ix[i:(i+window), 'mid_price'].values
        dy = np.diff(y)
        mod = sm.OLS(dy, sm.add_constant(y[:-1]))
        res = mod.fit()
        data.ix[(i+window):(i+window+step), 'hurst_exp_{}_{}'.format(window, step)] = res.params[0]
    return ['hurst_exp_{}_{}'.format(window, step)]