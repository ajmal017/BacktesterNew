import trading.futures_utils as fut
from ib.ext.Contract import Contract


def create_ib_futures_contract_from_symbol(symbol):
    """

    :param symbol:
    :return:
    """
    exp_year = fut.get_exp_year_from_symbol(symbol)
    exp_month = fut.get_exp_month_from_symbol(symbol)
    base_symbol = fut.get_base_symbol_from_symbol(symbol)
    return create_ib_futures_contract(base_symbol, exp_month, exp_year)


def create_ib_futures_contract(symbol, exp_month=1, exp_year=2016, exchange='NYMEX', currency='USD'):
    """
    Creates a futures contract used for IB orders.
    :param symbol: (string) base symbol e.g. 'CL'
    :param type: (string) 'FUT' --> futures
    :param exp_month: (int) e.g. 6 --> June
    :param exp_year: (int) e.g. 2016
    :param exchange:
    :param currency:
    :return:
    """
    # TODO: create IBContract class
    exp_month = str(exp_month)
    if len(exp_month) == 1:
        exp_month = '0' + exp_month
    exp_year = str(exp_year)
    expiry = exp_year + exp_month

    contract = Contract()
    contract.m_symbol = symbol
    contract.m_secType = 'FUT'
    contract.m_expiry = expiry
    contract.m_exchange = exchange
    contract.m_currency = currency
    return contract


def create_ib_stock_contract(symbol, exch='NASDAQ', prim_exch='NASDAQ', curr='USD'):
    """
    Create a stock contract used for IB orders.
    :param symbol:
    :param exch:
    :param prim_exch:
    :param curr:
    :return:
    """
    contract = Contract()
    contract.m_symbol = symbol
    contract.m_secType = 'STK'
    contract.m_exchange = exch
    contract.m_primaryExch = prim_exch
    contract.m_currency = curr
    print contract.__dict__
    return contract


def get_contract_details(contract):
    """
    Converts Contract into a dict
    :param contract:
    :return:
    """
    contract_details = {}
    # {'m_tradingClass': 'GC', 'm_conId': 79342391, 'm_symbol': 'GC',
    # 'm_secType': 'FUT', 'm_includeExpired': False, 'm_multiplier': '100',
    # 'm_primaryExch': 'NYMEX', 'm_right': '0', 'm_expiry': '20160628',
    # 'm_currency': 'USD', 'm_localSymbol': 'GCM6', 'm_strike': 0.0}
    contract_details['contract_id'] = contract.m_conId
    contract_details['symbol'] = contract.m_symbol
    contract_details['sec_type'] = contract.m_secType
    contract_details['expiry'] = contract.m_expiry
    contract_details['multiplier'] = contract.m_multiplier
    contract_details['exchange'] = contract.m_primaryExch
    contract_details['currency'] = contract.m_currency
    contract_details['sec_id_type'] = contract.m_secIdType
    contract_details['sec_id'] = contract.m_secId

    exp_year, exp_month, exp_day = get_exp(contract_details['expiry'])
    contract_details['ticker'] = fut.build_contract(contract_details['symbol'], exp_year, exp_month)
    return contract_details

def get_contract_ticker(contract):
    exp_year, exp_month, exp_day = get_exp(contract.m_expiry)
    ticker = fut.build_contract(contract.m_symbol, exp_year, exp_month)
    return ticker

def get_execution_details(execution):
    """
    Converts Execution into a dict
    :param execution:
    :return:
    """
    execution_details = dict()
    execution_details['order_id'] = execution.m_orderId
    execution_details['client_id'] = execution.m_clientId
    execution_details['execution_id'] = execution.m_execId
    execution_details['time'] = execution.m_time
    execution_details['account'] = execution.m_acctNumber
    execution_details['exchange'] = execution.m_exchange
    execution_details['side'] =  execution.m_side
    execution_details['qty'] = execution.m_shares
    execution_details['price'] = execution.m_price
    execution_details['cum_size'] = execution.m_cumQty
    execution_details['avg_price'] = execution.m_avgPrice
    execution_details['order_ref'] = execution.m_orderRef
    execution_details['rule'] = execution.m_evRule
    execution_details['multiplier'] = execution.m_evMultiplier
    return execution_details


def get_exp(exp_string):
    exp_year = int(exp_string[:4])
    exp_month = int(exp_string[4:6])
    exp_day = int(exp_string[6:])

    return exp_year, exp_month, exp_day
