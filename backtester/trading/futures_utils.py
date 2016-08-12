import os
import csv
import json
import datetime as dt
import Quandl as Qd
from dateutil.tz import tzlocal
from dateutil.relativedelta import relativedelta
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

QUANDL_KEY = "SyH7V4ywJGho77EC6W7C"


def _load_cme_month_codes():
    f_path = os.path.join(__location__, 'cme_month_codes.json')
    month_codes = json.load(open(f_path, 'r'))
    for k, v in month_codes.items():
        try:
            month_codes[int(k)] = v
        except ValueError:
            pass
    return month_codes


def get_contract_month_code(exp_month):
    """
    Get the CME contract month code.
    :param exp_month: (int)
    :return: (char)
    """
    month_codes = _load_cme_month_codes()
    return month_codes[exp_month]


def build_contract(symbol, exp_year, exp_month):
    """
    Build the contract ticker.
    :param symbol: (str)
    :param exp_year: (int)
    :param exp_month: (int)
    :return: (str) e.g. 'GCM6'
    """
    year = str(exp_year)[-1]
    month = get_contract_month_code(exp_month)
    return symbol + month + year


def get_exp_year_from_symbol(symbol):
    return int('201'+symbol[-1])


def get_exp_month_from_symbol(symbol):
    month_codes = _load_cme_month_codes()
    return month_codes[symbol[-2]]


def get_base_symbol_from_symbol(symbol):
    return symbol[:-2]


def get_quandl_future_code(symbol, exp_year, exp_month):
    """
    Builds the quandl database code for the requested future contract.
    :param symbol:
    :param exp_year:
    :param exp_month:
    :return:
    """
    return 'CME/' + symbol + get_contract_month_code(exp_month) + str(exp_year)


def get_futures_data(symbol, exp_year, exp_month):
    """
    Get's inter-day futures data from Quandl.
    :param symbol: (str)
    :param exp_year: (int)
    :param exp_month: (int)
    :return: (DataFrame)
    |                     |   Open |   High |    Low |   Last |   Change |   Settle |   Volume |   Open Interest |
    |:--------------------|-------:|-------:|-------:|-------:|---------:|---------:|---------:|----------------:|
    | 2016-02-26 00:00:00 |  nan   |  nan   |  nan   |  nan   |      0   |   1220.7 |        0 |               0 |
    | 2016-02-29 00:00:00 | 1220.2 | 1241.4 | 1220.2 | 1241.4 |     14   |   1234.7 |       49 |               0 |
    | 2016-03-01 00:00:00 | 1245.3 | 1247   | 1229.7 | 1232.4 |      3.6 |   1231.1 |       42 |              32 |
    | 2016-03-02 00:00:00 | 1227.8 | 1241.1 | 1227.8 | 1240.9 |     11   |   1242.1 |       42 |              33 |
    | 2016-03-03 00:00:00 | 1243.8 | 1268.1 | 1243.2 | 1268.1 |     16.5 |   1258.6 |       36 |              32 |
    | 2016-03-04 00:00:00 | 1265   | 1279.6 | 1253.5 | 1259.8 |     12.5 |   1271.1 |      113 |              37 |
    | 2016-03-07 00:00:00 | 1267.2 | 1270   | 1260.2 | 1263.8 |      6.6 |   1264.5 |       37 |              98 |
    | 2016-03-08 00:00:00 | 1269.5 | 1279.2 | 1261.4 | 1262.4 |      1.1 |   1263.4 |       69 |             118 |
    | 2016-03-09 00:00:00 | 1262.6 | 1263.1 | 1245   | 1252.2 |      5.5 |   1257.9 |      227 |             157 |
    | 2016-03-10 00:00:00 | 1250.6 | 1274.5 | 1240.1 | 1274   |     15.4 |   1273.3 |      249 |             309 |
    """
    quandl_future_code = get_quandl_future_code(symbol, exp_year, exp_month)
    return Qd.get(dataset=quandl_future_code, authtoken=QUANDL_KEY)


def get_highest_volume_contract(symbol, year, month, day):
    """
    Get the highest-volume contract for the symbol for a given date.
    :param symbol: (int)
    :param year: (int)
    :param month: (int)
    :param day: (int)
    :return: (str) e.g. 'GCM6'
    """
    highest_volume_contract = build_contract(symbol, year, month)
    max_volume = 0
    start_date = dt.datetime(year, month, day)
    for i in range(8):
        date = start_date + relativedelta(months=i)
        try:
            data = get_futures_data(symbol, date.year, date.month)
            volume = data.ix[start_date]['Volume']
            if volume >= max_volume:
                highest_volume_contract = build_contract(symbol, date.year, date.month)
                max_volume = volume
        except Qd.DatasetNotFound:
            pass

    return highest_volume_contract


def get_contract_specs(symbol):
    """

    :param symbol:
    :return:
    """
    reader = csv.DictReader(open(os.path.join(__location__, 'contract_specs.csv')))
    contracts = {}
    for row in reader:
        k = row['Symbol']
        contracts[k] = row
    specs = contracts[symbol]
    return specs


def get_mkt_times(trading_times_str):
    mkt_open_str = trading_times_str.split('-')[0].rstrip()
    mkt_close_str = trading_times_str.split('-')[1].lstrip()
    mkt_open = dt.datetime.strptime(mkt_open_str, '%H:%M').replace(tzinfo=tzlocal()).time()
    mkt_close = dt.datetime.strptime(mkt_close_str, '%H:%M').replace(tzinfo=tzlocal()).time()
    return mkt_open, mkt_close
