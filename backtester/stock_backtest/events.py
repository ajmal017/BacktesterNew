from trading import events


class StockBacktestFillEvent(events.FillEvent):
    def __init__(self, fill_time, symbol, quantity, fill_price, fill_cost, commission=0):
        super(StockBacktestFillEvent, self).__init__(fill_time, symbol, quantity, fill_price, fill_cost,
                                                     exchange='StockBacktestExecution',
                                                     commission=commission)