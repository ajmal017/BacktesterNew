import json
from Queue import Empty
from data_handler import StockBacktestDataHandler
from execution_handler import StockBacktestExecutionHandler
from trading.backtest import Backtest


class StockBacktest(Backtest):
    def __init__(self, events, strategy, data, execution, start_date, end_date, analytics=None, initial_capital=1000000):
        """
        :param analytics:
        :param events: (Queue)
        :param strategy: (Strategy)
        :param data: (DataHandler)
        :param execution: (ExecutionHandler)
        :param start_date: (DateTime)
        :param end_date: (DateTime)
        """
        assert isinstance(data, StockBacktestDataHandler)
        assert isinstance(execution, StockBacktestExecutionHandler)
        super(StockBacktest, self).__init__(events, strategy, data, execution, start_date, end_date, analytics)
        self.initial_capital = initial_capital

    def event_handler(self):
        event_handlers = {
            'MARKET': self._handle_market_event,
            'ORDER': self._handle_order_event,
            'FILL': self._handle_fill_event
        }
        while True:
            if self.data.continue_backtest:
                self.data.update()
            else:
                self.strategy.finished()
                return
            while True:
                try:
                    event = self.events.get(False)
                except Empty:
                    break
                else:
                    if event is not None:
                        event_handlers[event.type](event)

    def _handle_market_event(self, market_event):
        self.strategy.new_tick_update(market_event)
        self.strategy.new_tick()
        self.execution.process_resting_orders(market_event)

    def _handle_order_event(self, order_event):
        self.execution.process_new_order(order_event)

    def _handle_fill_event(self, fill_event):
        self.strategy.new_fill_update(fill_event)
        self.strategy.new_fill(fill_event)

