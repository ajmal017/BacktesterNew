import os
import datetime as dt
import pandas as pd
import pandas_datareader.data as web
from collections import OrderedDict


def get_stock_data(symbol, start_date=None, end_date=None):
    """
    Get daily resolution O/H/L/C stock data from Yahoo Finance for a single symbol.
    If start_date and end_date are None, get's all possible data.
    :param symbol: (string)
    :param start_date: (DateTime)
    :param end_date: (DateTime)
    :return: (DataFrame)
    |                     |   Open |    High |     Low |   Close |     Volume |   Adj Close |
    |:--------------------|-------:|--------:|--------:|--------:|-----------:|------------:|
    | 2016-01-04 00:00:00 | 743    | 744.06  | 731.258 |  741.84 | 3.2728e+06 |      741.84 |
    | 2016-01-05 00:00:00 | 746.45 | 752     | 738.64  |  742.58 | 1.9507e+06 |      742.58 |
    | 2016-01-06 00:00:00 | 730    | 747.18  | 728.92  |  743.62 | 1.947e+06  |      743.62 |
    | 2016-01-07 00:00:00 | 730.31 | 738.5   | 719.06  |  726.39 | 2.9637e+06 |      726.39 |
    | 2016-01-08 00:00:00 | 731.45 | 733.23  | 713     |  714.47 | 2.4509e+06 |      714.47 |
    | 2016-01-11 00:00:00 | 716.61 | 718.855 | 703.54  |  716.03 | 2.0906e+06 |      716.03 |
    | 2016-01-12 00:00:00 | 721.68 | 728.75  | 717.317 |  726.07 | 2.0245e+06 |      726.07 |
    | 2016-01-13 00:00:00 | 730.85 | 734.74  | 698.61  |  700.56 | 2.5017e+06 |      700.56 |
    | 2016-01-14 00:00:00 | 705.38 | 721.925 | 689.1   |  714.72 | 2.2258e+06 |      714.72 |
    | 2016-01-15 00:00:00 | 692.29 | 706.74  | 685.37  |  694.45 | 3.5924e+06 |      694.45 |
        """
    if start_date is not None and end_date is not None:
        assert start_date < end_date, "Start date is later than end date."
    symbol_data = web.DataReader(symbol, 'yahoo', start_date, end_date)
    return symbol_data


def get_stock_data_multiple(symbols, start_date=None, end_date=None):
    """
    Get daily resolution O/H/L/C stock data from Yahoo Finance for multiple symbols.
    :param symbols: (list) of symbols (str)
    :param start_date: (DateTime)
    :param end_date: (DateTime)
    :return: (OrderedDict) of DataFrames of stock data from start_date to end_date
    """
    data = OrderedDict()
    for symbol in symbols:
        symbol_data = get_stock_data(symbol, start_date, end_date)
        data[symbol] = symbol_data

    return data


def get_pct_returns(symbol, start_date=None, end_date=None, col='Adj Close'):
    """

    :param symbol: (string)
    :param start_date: (datetime
    :param end_date:
    :param col: (string) name of column to calculate the pct returns from
    :return:
    """
    data = get_stock_data(symbol, start_date, end_date)[col]
    return data.pct_change().fillna(0)


def get_returns(symbol, start_date=None, end_date=None, col='Adj Close'):
    """

    :param symbol:
    :param start_date:
    :param end_date:
    :param col:  (string) name of column to calculate the returns from
    :return:
    """
    data = get_stock_data(symbol, start_date, end_date)[col]
    return data.diff().fillna(0)


def get_company_name(symbol):
    """
    Get the full name of the company by the symbol.
    :param symbol:
    :return:
    """
    f_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'secwiki_tickers.csv')
    df = pd.read_csv(f_path)
    company_info = df[df.Ticker == symbol]
    code = company_info['Name'].keys()[0]
    company_name = company_info.to_dict()['Name'][code]
    return company_name


def get_company_sector(symbol):
    """
    Get the sector of the company by the symbol.
    :param symbol: (str)
    :return: (str)
    """
    f_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'secwiki_tickers.csv')
    df = pd.read_csv(f_path)
    company_info = df[df.Ticker == symbol]
    code = company_info['Name'].keys()[0]
    company_sector = company_info.to_dict()['Sector'][code]
    return company_sector