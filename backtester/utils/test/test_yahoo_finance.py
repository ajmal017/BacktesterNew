import unittest
import datetime as dt
import utils.data_utils.yahoo_finance as yf


class TestYahooFinance(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.symbol = 'GOOG'
        cls.symbol2 = 'AAPL'
        cls.start_date = dt.datetime(year=2016, month=1, day=1)
        cls.end_date = dt.datetime(year=2016, month=2, day=1)
        cls.test_date = dt.datetime(year=2016, month=2, day=1)

    def test_get_stock_data(self):
        data = yf.get_stock_data(self.symbol, self.start_date, self.end_date)
        self.assertTrue(data.keys().__contains__('Open'))
        self.assertTrue(data.keys().__contains__('High'))
        self.assertTrue(data.keys().__contains__('Low'))
        self.assertTrue(data.keys().__contains__('Close'))
        self.assertTrue(data.keys().__contains__('Volume'))

    def test_get_pct_returns(self):
        pct_returns = yf.get_pct_returns(self.symbol, self.start_date, self.end_date)
        self.assertAlmostEqual(pct_returns[self.test_date], .0121811533129)

    def test_get_returns(self):
        returns = yf.get_returns(self.symbol, self.start_date, self.end_date)
        self.assertAlmostEqual(returns[self.test_date], 9.049988)

    def test_get_company_name(self):
        company_name = yf.get_company_name(self.symbol)
        self.assertEqual('Google Inc.', company_name)

    def test_get_company_sector(self):
        company_sector = yf.get_company_sector(self.symbol)
        self.assertEqual('Technology', company_sector)
