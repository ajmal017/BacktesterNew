import os
from service_cli.qgservice import QuantGoService


subscription_map = {
    "CME_Level_2": 6802
}


def _file_empty(fpath):
    return os.stat(fpath).st_size == 0


# TODO - determine if no data was retrieved
def download_data(symbol, date, subscription, fpath):
    """
    symbol - ticker symbol (string)
    date - datetime object
    subscription - the data subscription type (i.e CME_Level_2)
    """
    date_str = date.strftime("%Y%m%d")
    symbol = symbol.upper()

    qg = QuantGoService()
    service = subscription_map[subscription]

    if not os.path.exists(os.path.dirname(fpath)):
        os.makedirs(os.path.dirname(fpath))

    with open(fpath, 'wb') as output_file:
        qg.get_data(service,
                    {"Header": True,
                     "TickerNames": [symbol],
                     "Date": date_str,
                     "ServiceParameters": {"IncludeDate": True}},
                    output_file)

    if _file_empty(fpath):
        os.remove(fpath)
        raise IOError("No data found, file is empty")

    return fpath