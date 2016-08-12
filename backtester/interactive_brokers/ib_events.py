import datetime as dt
import trading.events as events

class IBMarketEvent(events.MarketEvent):
    def __init__(self, dt):
        super(IBMarketEvent, self).__init__(dt)


class IBFillEvent(events.FillEvent):
    """
    Subclasses FillEvent and also contains field with dicts for execution_details and contract_details.
    """
    def __init__(self, execution, contract):
        """
        :param execution: (dict) execution details
        :param contract: (dict) contract details
        """
        super(IBFillEvent, self).__init__(fill_time=execution['time'],
                                          symbol=contract['ticker'],
                                          quantity=execution['qty'],
                                          fill_price=execution['avg_price'],
                                          fill_cost=execution['qty']*execution['avg_price'],
                                          exchange=execution['exchange'],
                                          commission=0)
        # TODO: commission
        self.execution = execution
        self.contract = contract

class IBOpenOrderEvent(events.Event):
    def __init__(self):
        pass

class IBCommissionReportEvent(events.Event):
    def __init__(self):
        pass


