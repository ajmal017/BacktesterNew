import datetime as dt
from abc import ABCMeta, abstractmethod
from collections import OrderedDict

class Event(object):
    """
    Types include:
        MARKET:
        NEW_DAY
        ORDER
        FILL
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def info(self):
        """
        For logging purposes.
        :return: (dict)
        """
        raise NotImplementedError("Event.info()")

class MarketEvent(Event):
    """
    Handles the event of receiving a new market update.
    """
    def __init__(self, dt, data):
        self.type = 'MARKET'
        self.dt = dt
        self.data = data

    def info(self):
        return None

# class NewDayEvent(Event):
#     def __init__(self, prev_data, next_date):
#         """
#         New day, gives previous day's data, called only when there is a previous day existing
#         :param prev_data: (datetime)
#         :param next_date: (datetime)
#         """
#         self.type = 'NEW_DAY'
#         self.prev_data = prev_data
#         self.prev_date = prev_data.reset_index()['time'].values[0]
#         self.next_date = next_date


class OrderEvent(Event):
    """
    Handles the event of sending an Order to an execution system.
    :param order_time: (DateTime) order time
    :param symbol: (str)
    :param order_type: (str) 'MARKET', 'LIMIT'
    :param quantity: (int)
    :param price: (float)
    """
    def __init__(self, product, quantity, order_type='MARKET', price=None, order_time=None):
        self.type = 'ORDER'
        self.product = product
        self.symbol = product.symbol
        assert order_type == 'MARKET' or order_type == 'LIMIT'
        self.order_type = order_type
        self.quantity = quantity
        self.price = price
        if order_type == 'LIMIT':
            assert price is not None, "LIMIT order must have a price."
            try:
                self.price = float(self.price)
            except TypeError:
                print "LIMIT order has invalid price."

        self.order_time = order_time if order_time is not None else dt.datetime.now()

    def __str__(self):
        return "ORDER | Symbol: {}, Qty: {}, Type: {}, Time: {}"\
            .format(self.symbol, self.quantity, self.quantity, self.order_type, self.order_time)

    def info(self):
        return {
            'dt': self.order_time.strftime("%-m/%-d/%Y %H:%M"),
            'type': self.order_type,
            'symbol': self.symbol,
            'quantity': self.quantity,
            'price': self.price,
        }

    @classmethod
    def from_json(cls):
        raise NotImplementedError()


class FillEvent(Event):
    def __init__(self, fill_time, symbol, quantity, fill_price, fill_cost, exchange, commission=0):
        """
        Encapsulates the notion of a Filled Order, as returned from a brokerage.
        Stores the quantity of an instrument actually filled and at what price.
        In addition, stores the commission of the trade from the brokerage.

        :param fill_time: (DateTime) fill time
        :param symbol: (str)
        :param quantity: (int)
        :param fill_cost: (float)
        :param exchange: (str)
        :param commission: (float)
        :return:
        """
        self.type = 'FILL'
        self.fill_time = fill_time
        self.symbol = symbol
        self.quantity = quantity
        self.fill_price = fill_price
        self.fill_cost = fill_cost
        self.exchange = exchange
        self.commission = commission

    def info(self):
        return {
            'dt': self.fill_time.strftime(("%-m/%-d/%Y %H:%M")),
            'symbol': self.symbol,
            'quantity': self.quantity,
            'cost': self.fill_cost,
            'exchange': self.exchange,
            'commission': self.commission
        }