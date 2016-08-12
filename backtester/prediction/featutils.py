import numpy as np

__author__ = 'Greg'


def label_data(data, pred_col, thresh, label_name='label'):
    data[label_name] = 1
    data.loc[data[pred_col] > thresh, label_name] = 1
    data.loc[data[pred_col] < -thresh, label_name] = -1


def label_data_future_pnl(data, pred_col_prefix, pred_col_suffix, thresh, label_name='label'):
    buy_pnl = "{}_buy_{}".format(pred_col_prefix, pred_col_suffix)
    sell_pnl = "{}_sell_{}".format(pred_col_prefix, pred_col_suffix)
    data[label_name] = 0
    data.loc[data[buy_pnl] > thresh, label_name] = 1
    data.loc[data[sell_pnl] > thresh, label_name] = -1
    data.loc[(data[buy_pnl] > thresh) & (data[sell_pnl] > thresh), label_name] = 2


def standardize_features(data, feature_names):
    std_feats = []
    for feat in feature_names:
        if data[feat].dtype != np.bool:
            std_feats.append(feat)
    data[std_feats] = (data[std_feats] - data[std_feats].mean()) / data[std_feats].std()


def add_mid_price(data):
    data['mid_price'] = (data['level_1_price_buy'] + data['level_1_price_sell']) / 2
    return ['mid_price']


def add_log_returns(data):
    if 'mid_price' not in data.columns:
        add_mid_price(data)
    data['log_returns'] = np.concatenate([[0], np.diff(np.log(data['mid_price']))])
    return ['log_returns']


def drop_orderbook(min_keep_level=0, depth=5):
    drop_cols = []
    for i in xrange(min_keep_level+1, depth+1):
        for side in ['buy', 'sell']:
            drop_cols += ['level_{}_price_{}'.format(i, side),
                          'level_{}_volume_{}'.format(i, side),
                          'level_{}_orders_{}'.format(i, side)]
    return drop_cols


def multi_window(feat, windows, *args, **kwargs):
    return lambda data: sum([feat(*args, window=window, **kwargs)(data) for window in windows], [])