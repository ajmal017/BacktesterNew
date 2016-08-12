import os
import re
import pandas as pd
import datetime as dt
from dateutil.rrule import rrule, DAILY
from cme_backtest.data_utils.data_aggregation import make_second_bars, make_concise
from cme_backtest.data_utils.data_path import get_file_path
from cme_backtest.data_utils.quantgo_download import download_data


def _convert_time(date_str, time_str):
    return dt.datetime.strptime(date_str + time_str + "000", '%Y%m%d %H%M%S%f')


def _parse_cme_level2_data(fpath, max_level=5):
    """
    parse level 2 data
    columns are:
        (time, symbol, is_implied) as index
        +
        buy_depth, sell_depth,
        +
        for 1 <= i <= 10 and side in ['buy', 'sell'] (level_i_price_side, level_i_volume_side, level_i_orders_side)
    """
    index_names = ['time', 'side', 'is_implied']
    column_names = ['symbol', 'depth'] + map(lambda x: "level_{}".format(x), range(1, 11))
    data = pd.read_csv(fpath, parse_dates=[[0, 1]], date_parser=_convert_time, index_col=[0, 2, 3])
    data.index.names = index_names
    data.columns = column_names

    if len(data) == 0:
        raise IOError("File is empty")

    for i in xrange(1, max_level + 1):
        d = zip(*data["level_{}".format(i)].apply(lambda s: re.split(r' x | \(|\)', s)[:3]).tolist())
        data['level_{}_price'.format(i)] = map(float, d[0])
        data['level_{}_volume'.format(i)] = map(int, d[1])
        data['level_{}_orders'.format(i)] = map(int, d[2])
        data.drop("level_{}".format(i), axis=1, inplace=True)

    data['symbol'] = data['symbol'].apply(lambda s: s.replace(' ', ''))
    data = data.reset_index()
    data = data[data['is_implied'] == 0]
    buy_data = data[data['side'] == data['side'].values[0]].drop('side', axis=1)
    sell_data = data[data['side'] != data['side'].values[0]].drop('side', axis=1)
    data = pd.ordered_merge(buy_data, sell_data, on=['time', 'symbol'], fill_method='ffill', suffixes=['_buy', '_sell'])
    data.set_index('time', inplace=True)

    return data


def _download_and_parse(date, download, fpath, subscription, symbols):
    if not os.path.exists(fpath):
        if not download:
            raise ValueError("Data for {} on {} is missing, set download=True".format(symbols, date))
        download_data(symbols, date, subscription, fpath)

    # load data to pandas and do initial parsing
    if subscription == 'CME_Level_2':
        data = _parse_cme_level2_data(fpath)
    else:
        raise NotImplementedError("Parser for {} not supported".format(subscription))

    return data


def get_data(symbol, date, download=False, save=True, parse_new=False, second_bars=True, subscription="CME_Level_2",
             start_time=dt.time(0, 0, 0), end_time=dt.time(23, 59, 59), concise=False):
    """
    :param symbol: (str)
    :param date: (DateTime) or (str) e.g. 2015-12-22
    :param download: (bool)
    :param save: (bool)
    :param parse_new: (bool)
    :param second_bars: (bool)
    :param subscription: (str) the data subscription type (e.g. CME_Level_2)
    :param start_time: (DateTime)
    :param end_time: (DateTime)
    :param concise: (bool)
    :return:
    """

    if type(date) is str:
        date = dt.datetime.strptime(date, "%Y-%m-%d")

    fpath = get_file_path(symbol, date, subscription)
    parsed_fpath = get_file_path(symbol, date, subscription, extension='_parsed')
    second_fpath = get_file_path(symbol, date, subscription, extension='_second_bars')
    if second_bars and os.path.exists(second_fpath):
        data = pd.read_csv(second_fpath, parse_dates=[0], index_col=[0])
    else:
        if os.path.exists(parsed_fpath) and not parse_new:
            data = pd.read_csv(parsed_fpath, parse_dates=[0], index_col=[0])
            data['symbol'] = data['symbol'].apply(lambda s: s.replace(' ', ''))
        else:
            data = _download_and_parse(date, download, fpath, subscription, symbol)
            if save:
                data.to_csv(parsed_fpath)
        if second_bars:
            data = make_second_bars(data, subscription, save=True, load_if_exists=True)

    # slice on time index
    start_date = date + dt.timedelta(hours=start_time.hour, minutes=start_time.minute, seconds=start_time.second,
                                     microseconds=start_time.microsecond)
    end_date = date + dt.timedelta(hours=end_time.hour, minutes=end_time.minute, seconds=end_time.second,
                                   microseconds=end_time.microsecond)
    data = data[(data.index >= start_date) & (data.index <= end_date)]

    if concise is True:
        data = make_concise(data)

    data['dt'] = data.index

    if len(data) <= 1:
        raise ValueError("Bad data for {} - {}".format(symbol, date.strftime("%Y-%m-%d")))

    data.index.name = 'time'
    return data


def get_data_multi(symbols, date, download=False, save=True, parse_new=False, second_bars=True,
                   subscription="CME_Level_2", start_time=dt.time(0, 0, 0), end_time=dt.time(23, 59, 59),
                   concise=False):
    """
    Returns a merged multi-index DataFrame of all symbols in symbols
    :param symbols:
    :param date:
    :param download:
    :param save:
    :param parse_new:
    :param second_bars:
    :param subscription:
    :param start_time:
    :param end_time:
    :param concise:

    :return: (Multi-Index DataFrame)
    """
    multi_data = {}
    if len(symbols) == 1:
        multi_data[symbols[0]] = get_data(symbols[0], date, download, save, parse_new, second_bars,
                                          subscription, start_time, end_time, concise)
    else:
        for symbol in symbols:
            data = get_data(symbol, date, download, save, parse_new, second_bars,
                            subscription, start_time, end_time, concise)
            multi_data[symbol] = data

    multi_data = _reindex_data(multi_data)
    return dict_to_df(multi_data)


def dict_to_df(data):
    """
    Converts a dict of DataFrames to a multi-indexed DataFrame
    :param data: (dict of DataFrames)
    :return: (Multi-Indexed DataFrame)
    """
    reform = {(outerKey, innerKey): values for outerKey, innerDict in data.iteritems()
              for innerKey, values in innerDict.iteritems()}
    multi_data = pd.DataFrame(reform).ffill()
    return multi_data


def _reindex_data(data):
    """
    Re-indexes all DataFrames in datas (dict of DataFrames - symbol:df) and forward fills
    Used for combining multi-index data for multiple symbols
    """
    keys = data.keys()
    if len(keys) == 1:
        return data
    new_index = data[keys[0]].index.union(data[keys[1]].index).unique()  # merge first two
    if len(keys) >= 2:
        for i in range(2, len(keys)):
            new_index = new_index.union(data[keys[i]].index).unique()
    for key in keys:
        data[key] = data[key].apply(lambda x: x.asof(new_index))
    return data


def get_data_furdays(symbols, start_date, end_date, raise_exception=False, **kwargs):
    data = []
    for date in rrule(DAILY, dtstart=start_date, until=end_date):
        try:
            data.append(get_data(symbols, date, **kwargs))
        except (IOError, ValueError) as e:
            # data not found for day, skip
            if raise_exception:
                raise e
            else:
                pass
    return data


def merge_data(left_data, right_data, suffixes=('', None)):
    """
    merge two different securities, indexed on left_data
    """
    left_data = left_data.reset_index()
    right_data = right_data.reset_index()
    left_suffix, right_suffix = suffixes
    if right_suffix is None:
        right_sym = right_data['symbol'].values[0][:2].lower()
        right_suffix = '_{}'.format(right_sym)
    merged_data = pd.ordered_merge(left_data, right_data, fill_method='ffill', on='time', suffixes=(left_suffix, right_suffix)).fillna(0).set_index('time')
    merged_data = merged_data.ix[left_data['time'].values].fillna(0)
    return merged_data
