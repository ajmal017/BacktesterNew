import datetime as dt
import utils.yahoo_finance as yf
from trading.events import MarketEvent
from trading.data_handler import BacktestDataHandler
from bokeh.client import push_session
from bokeh.plotting import figure, curdoc, vplot

class StockBacktestDataHandler(BacktestDataHandler):
    def __init__(self, events, products, start_date, end_date, **kwargs):
        """
        Handles data for (one) stock using pandas/yahoo finance API.
        :param events: (Queue)
        :param products: (list)
        :param start_date: (DateTime)
        :param end_date: (DateTime)
        :return:
        """
        super(StockBacktestDataHandler, self).__init__(events, products, start_date, end_date)
        self.products = products
        self.symbols = [product.symbol for product in self.products]
        self.all_symbol_data = yf.get_stock_data_multiple(self.symbols, start_date=start_date, end_date=end_date)
        self.continue_backtest = True

        self.curr_dt = start_date
        self.last_bar = {product.symbol: {} for product in self.products}

        # if kwargs['plot_mkt_data'] is True:
        #     p = figure(x_axis_type="datetime", toolbar_location=None)
        #     p.background_fill_color = 'white'
        #     p.outline_line_color = 'black'
        #     p.grid.grid_line_color = 'gray'
        #     r = p.line(x=[], y=[])
        #     self.ds = r.data_source
        #     session = push_session(curdoc())
        #     curdoc().add_periodic_callback(self.mkt_data_callback, 50)
        #     session.show()

    def mkt_data_callback(self, x_val, y_val):
        self.ds.data['x'].append(x_val)
        self.ds.data['y'].append(y_val)
        self.ds.trigger('data', self.ds.data, self.ds.data)


    def update(self):
        if self.curr_dt > self.end_date:
            self.continue_backtest = False
            return
        try:
            self._push_next_data()
            self.curr_dt += dt.timedelta(days=1)
        except KeyError:
            self.curr_dt += dt.timedelta(days=1)
            self.update()

    def _push_next_data(self):
        """
        Push the next tick for all symbols.
        """
        for symbol in self.symbols:
            self.last_bar[symbol] = self.all_symbol_data[symbol].ix[self.curr_dt].to_dict()
        self.events.put(MarketEvent(self.curr_dt, self.last_bar))