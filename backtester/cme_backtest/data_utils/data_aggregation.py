import datetime as dt
import os

import pandas as pd

from cme_backtest.data_utils.data_path import get_file_path, get_date_and_sym


def make_second_bars(data, subscription='CME_Level_2', save=False, load_if_exists=True):

    date, sym = get_date_and_sym(data)
    fpath = get_file_path(sym, date, subscription, extension='_second_bars')

    if load_if_exists and os.path.exists(fpath):
        data = pd.read_csv(fpath, parse_dates=[0], index_col=[0])
    else:
        data = data.groupby(lambda x: dt.datetime(x.year, x.month, x.day, x.hour, x.minute, x.second)).mean()
        data['symbol'] = sym

        start_date = data.index[0]
        end_date = data.index[-1]

        time_index = pd.date_range(start=start_date, end=end_date, freq='s')
        data = data.reindex(time_index, method='ffill')

        data.index.name = 'time'

        if save:
            data.to_csv(fpath)

    return data


def make_concise(data):
    """
    Replace 'level_x_column' with just 'column' (list that represents the aggregate levels)
    :param data:
    :return:
    """
    data = data.apply(make_lists, axis=1)
    for i in range(1, 11):
        data = data.drop('level_'+str(i)+'_price_buy', axis=1)
        data = data.drop('level_'+str(i)+'_price_sell', axis=1)
        data = data.drop('level_'+str(i)+'_volume_buy', axis=1)
        data = data.drop('level_'+str(i)+'_volume_sell', axis=1)
        data = data.drop('level_'+str(i)+'_orders_buy', axis=1)
        data = data.drop('level_'+str(i)+'_orders_sell', axis=1)
    return data


def make_lists(bar):
    """
    Combines the individual 'level_x_column' into a list
    :param bar:
    :return:
    """
    buy_price = []
    sell_price = []
    buy_volume = []
    sell_volume = []
    buy_orders = []
    sell_orders = []

    for i in range(1, 11):
        try:
            buy_price.append(bar['level_'+str(i)+'_price_buy'])
            sell_price.append(bar['level_'+str(i)+'_price_sell'])
            buy_volume.append(bar['level_'+str(i)+'_volume_buy'])
            sell_volume.append(bar['level_'+str(i)+'_volume_sell'])
            buy_orders.append(bar['level_'+str(i)+'_orders_buy'])
            sell_orders.append(bar['level_'+str(i)+'_orders_sell'])
        except IndexError:
            pass
    bar['price_buy'] = buy_price
    bar['price_sell'] = sell_price
    bar['volume_buy'] = buy_volume
    bar['volume_sell'] = sell_volume
    bar['orders_buy'] = buy_orders
    bar['orders_sell'] = sell_orders

    return bar