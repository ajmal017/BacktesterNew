from trading import events

class CMEBacktestMarketEvent(events.MarketEvent):
    def __init__(self, dt):
        super(CMEBacktestMarketEvent, self).__init__(dt)

class CMEBacktestFillEvent(events.FillEvent):
    def __init__(self, order_time, fill_time, symbol, quantity, fill_price, fill_cost, commission=0):
        super(CMEBacktestFillEvent, self).__init__(fill_time, symbol, quantity, fill_price, fill_cost,
                                                   exchange='CMEBacktest', commission=commission)
        self.order_time = order_time

    def __str__(self):
        return "FILL  | Symbol: {}, OrderTime: {}, FillTime: {}, Qty: {}, Cost: {}, Exchange: {}, Commission: {}"\
            .format(self.symbol, self.order_time, self.fill_time, self.quantity, self.fill_cost, self.exchange,
                    self.commission)


# class CMEBacktestOrderEvent(events.OrderEvent):
#     def __init__(self, symbol, datetime, order_type, quantity, price=None):
#         super(CMEBacktestOrderEvent, self).__init__(symbol, order_type, quantity, price)
#         self.datetime = datetime
#
#     def __str__(self):
#         return "ORDER | Symbol: {}, Time: {}, Qty: {}, Type: {}"\
#             .format(self.symbol, self.datetime, self.quantity, self.order_type)