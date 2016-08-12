from abc import ABCMeta, abstractmethod


class ExecutionHandler(object):
    """
     The ExecutionHandler simulates a connection to a brokerage. The job of the handler
     is to take OrderEvents from the Queue and execute them, either via a simulated approach
     or an actual connection to a liver brokerage. Once orders are executed the handler
     creates FillEvents, which describe what was actually transacted, including fees,
     commission and slippage (if modelled).
    """

    __metaclass__ = ABCMeta

    def __init__(self, events):
        self.events = events

    @abstractmethod
    def process_new_order(self, order_event):
        """
        Process an incoming order and sends a FillEvent to events on fill.
        :param order_event: (Event)
        """
        raise NotImplementedError("ExecutionHandler.process_order()")
