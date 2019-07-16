#!/usr/bin/env python

import pandas as pd
from yahoofinancials import YahooFinancials as yf

class YahooData:

    def __init__(self, tickers=['AAPL', 'MSFT'], start_date='2018-01-01', end_date='2018-12-31'):
        self.start_date = start_date
        self.end_date = end_date
        self.tickers = tickers
        self._get_data()

    def _get_data(self):
        stock_data = yf(self.tickers)
        prices = stock_data.get_historical_price_data(self.start_date, self.end_date, 'daily')
        self.prices = prices



if __name__ == '__main__':
    yd = YahooData()
    all_prices = yd.prices
    for key, val in all_prices.items():
        prices = all_prices[key]['prices']
        df = pd.DataFrame(prices)
        df['ticker'] = key
        columns = ['high', 'low', 'adjclose', 'volume']
        df = pd.DataFrame(df[columns].values, index=([df['ticker'].values, df['formatted_date'].values]), columns=columns)
        print(df.head())


