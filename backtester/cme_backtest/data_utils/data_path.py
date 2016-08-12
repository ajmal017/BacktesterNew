import os


apply_n_times = lambda f, n, x: reduce(lambda x, y: f(x), range(n), x)

ROOT_DIR = apply_n_times(os.path.dirname, 3, os.path.realpath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, 'data')


def get_date_and_sym(data):
    date = data.index[0]
    sym = data['symbol'].values[0]
    return date, sym


def get_file_path(symbol, date, subscription, extension=''):
    date_str = date.strftime("%Y%m%d")
    symbol = symbol.upper()
    fpath = os.path.join(DATA_DIR, subscription, symbol, "{}{}.csv".format(date_str, extension))
    return fpath

