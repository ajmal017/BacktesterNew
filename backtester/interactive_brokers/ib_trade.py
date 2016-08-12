from queue import Empty

class IBTrade(object):
    def __init__(self, events, strategy, data, execution, **kwargs):
        self.events = events
        self.strategy = strategy
        self.data = data
        self.execution = execution
        self.running = True

    def run(self):
        self._log_trading_info()
        self.event_handler()

    def event_handler(self):
        event_handlers = {
            'MARKET': self._handle_market_event,
            'ORDER': self._handle_order_event,
            'FILL': self._handle_fill_event,
        }

        while True:
            if self.running:
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

    def _handle_order_event(self, order_event):
        self.execution.process_new_order(order_event)

    def _handle_fill_event(self, fill_event):
        self.strategy.new_fill_update(fill_event)
        self.strategy.new_fill(fill_event)


    def _log_trading_info(self):
        print "STARTING TRADING \n" \
              "Strategy: {} \n" \
              "Execution: {} \n"\
            .format(self.strategy.__class__.__name__,
                    self.execution.__class__.__name__)
