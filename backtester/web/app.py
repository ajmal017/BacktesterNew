import json
import random
import datetime as dt
from ib_web_account_handler import IBWebAccountHandler
from ib_web_data_handler import IBWebDataHandler
from queue import Queue
from flask import Flask, Response, render_template, make_response
from collections import OrderedDict

app = Flask(__name__)

events = Queue()
CONFIG = json.load(open('test_ib_config.json', 'r'))
ib_account_handler = IBWebAccountHandler(events, CONFIG)
# ib_data_handler = IBWebDataHandler(events, CONFIG)

last_int = 500

@app.route('/')
def dashboard():
    return render_template('index.html', positions=ib_account_handler.portfolio.keys())
    # return render_template('index.html')

#
@app.route('/account_info')
def account_info():
    info = ib_account_handler.account_info
    response = make_response(json.dumps(info))
    response.content_type = 'application/json'
    return response

@app.route('/portfolio_info')
def portfolio_info():
    info = ib_account_handler.portfolio
    response = make_response(json.dumps(info))
    response.content_type = 'application/json'
    return response

@app.route('/chart')
def chart():
    return render_template('chart.html')

trades = []

@app.route('/random_data')
def random_data():
    # info = ib_account_handler.account_info
    # response = make_response(json.dumps(info))
    global last_int, trades
    direction = random.randint(0, 1)
    if direction == 1:
        last_int += 20
    else:
        last_int -= 20
    # trades.append({'date': int(dt.datetime.now().strftime("%s")) * 1000 ,
    #          'price': float(last_int)})
    # response = make_response(json.dumps(trades))
    response = make_response(json.dumps(last_int))
    response.content_type = 'application/json'
    return response

if __name__ == '__main__':
    app.run(debug=True, threaded=True)
