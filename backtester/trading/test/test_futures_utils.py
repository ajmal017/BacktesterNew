import unittest
import datetime as dt
import trading.futures_utils as fut


class TestFuturesUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.symbol = 'GC'
        cls.full_symbol = 'GCM6'
        cls.exp_year = 2016
        cls.exp_month = 5
        cls.curr_year = 2016
        cls.curr_month = 4
        cls.curr_day = 6

    def test_get_contract_month_code(self):
        months = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
        for i in range(12):
            self.assertTrue(fut.get_contract_month_code(i + 1), months[i])

    def test_build_contract(self):
        contract = fut.build_contract(self.symbol, self.exp_year, self.exp_month)
        self.assertEqual('GCK6', contract)

    def test_get_exp_year_from_symbol(self):
        self.assertEqual(fut.get_exp_year_from_symbol(self.full_symbol), 2016)

    def test_get_exp_month_from_symbol(self):
        self.assertEqual(fut.get_exp_month_from_symbol(self.full_symbol), 6)

    def test_get_base_symbol_from_symbol(self):
        self.assertEqual(fut.get_base_symbol_from_symbol(self.full_symbol), 'GC')

    def test_get_quandl_future_code(self):
        quandl_future_code = fut.get_quandl_future_code(self.symbol, self.exp_year, self.exp_month)
        self.assertEqual('CME/GCK2016', quandl_future_code)

    def test_get_futures_data(self):
        test_data = fut.get_futures_data(self.symbol, self.exp_year, self.exp_month)
        test_date = dt.datetime(year=2016, month=3, day=1)
        test_data_date = test_data.ix[test_date]
        self.assertEqual(1245.3, test_data_date['Open'])
        self.assertEqual(1247.0, test_data_date['High'])
        self.assertEqual(1229.7, test_data_date['Low'])
        self.assertEqual(1232.4, test_data_date['Last'])
        self.assertEqual(3.6, test_data_date['Change'])
        self.assertEqual(1231.1, test_data_date['Settle'])
        self.assertEqual(42.0, test_data_date['Volume'])
        self.assertEqual(32.0, test_data_date['Open Interest'])

    def test_get_highest_volume_contract(self):
        highest_volume_contract = fut.get_highest_volume_contract(self.symbol, self.curr_year, self.curr_month,
                                                                  self.curr_day)
        self.assertEqual('GCM6', highest_volume_contract)

    def test_get_contract_specs(self):
        specs = fut.get_contract_specs('CL')
        print specs
        self.assertEqual(specs['Name'], 'Gold-COMEX')
        self.assertEqual(specs['Exchange'], 'NYMEX')
        self.assertEqual(specs['Quandl Code'], 'CME/GC')
        self.assertEqual(specs['Symbol'], 'GC')
        self.assertEqual(specs['Tick Value'], '10')
        self.assertEqual(specs['Contract Size'], '100 oz troy')
        self.assertEqual(specs['Active'], '1')
        self.assertEqual(specs['Delivery Months'], 'GHJKMQVZ')
        self.assertEqual(specs['Session Type'], 'Active')
        self.assertEqual(specs['Start Date'], '1/2/1975')
        self.assertEqual(specs['Units'], 'USD/troy oz')
        self.assertEqual(specs['Currency'], 'USD')
        self.assertEqual(specs['Trading Times'], '18:00 - 17:15')
        self.assertEqual(specs['Minimum Tick Value'], '1')
        self.assertEqual(specs['Full Point Value'], '100')
        self.assertEqual(specs['Terminal Point Value'], '10')

    def test_get_mkt_times(self):
        mkt_open, mkt_close = fut.get_mkt_times('18:00 - 17:15')
        mkt_open_expected = dt.datetime(year=2016, month=1, day=1, hour=18, minute=0).time()
        mkt_close_expected = dt.datetime(year=2016, month=1, day=1, hour=17, minute=15).time()
        self.assertEqual(mkt_open, mkt_open_expected)
        self.assertEqual(mkt_close, mkt_close_expected)
