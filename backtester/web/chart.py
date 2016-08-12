import random
import datetime as dt
import numpy as np
from bokeh.client import push_session
from bokeh.plotting import figure, curdoc


p1 = figure(x_axis_type = "datetime")
p1.title = "Stock Closing Prices"
p1.grid.grid_line_alpha=0.3
p1.xaxis.axis_label = 'Date'
p1.yaxis.axis_label = 'Price'

i = 0
point = 500
r1 = p1.line(x=[], y=[], color='#A6CEE3')

session = push_session(curdoc())
ds = r1.data_source
def update_tick():
    global point, i
    direction = random.randint(0, 1)
    if direction == 1:
        point += 1
    else:
        point -= 1

    ds.data['x'].append(dt.datetime.now().strftime('%s'))
    ds.data['y'].append(point)
    if len(ds.data['x']) > 100:
        del ds.data['x'][0]
        del ds.data['y'][0]
    i += 1
    ds.trigger('data', ds.data, ds.data)

curdoc().add_periodic_callback(update_tick, 50)
session.show() # open the document in a browser
session.loop_until_closed() # run forever