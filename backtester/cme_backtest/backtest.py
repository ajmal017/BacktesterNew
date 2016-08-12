from Queue import Empty
from data_handler import CMEBacktestDataHandler
from trading.backtest import Backtest


class CMEBacktest(Backtest):
    def __init__(self, events, strategy, data, execution, start_date, end_date, analytics=None,
                 start_time=None, end_time=None):
        assert isinstance(data, CMEBacktestDataHandler)
        super(CMEBacktest, self).__init__(events, strategy, data, execution, start_date, end_date, analytics)
        self.start_time = start_time
        self.end_time = end_time
        self.cash = 0

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
                print 'Backtest finished.'
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
