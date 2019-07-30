#!/usr/bin/env python
# Gets stock data from the world wide webs

import datetime as dt
import multiprocessing as mp
import os
import pandas as pd
from pandas_finance import Equity
from tqdm import tqdm
from yahoofinancials import YahooFinancials as yf

class YahooData:

    def __init__(self, ticker='AAPL'):
        self.ticker = ticker
        self.YF = yf(ticker)
        self.PF = Equity(ticker)
        self.date_str = dt.date.today().strftime('%Y-%m-%d')
        self.prices_dir = 'prices/prices_{}'.format(self.date_str)
        self.info_dir = 'info/info_{}'.format(self.date_str)

    def cache_price_data(self):

        prices = self.PF.trading_data
        if not os.path.exists(self.prices_dir):
            os.mkdir(self.prices_dir)
        prices.to_csv('{}/{}.csv'.format(self.prices_dir, self.ticker))

    def cache_cross_sectional_data(self):
        data = {
            'ticker': self.ticker,
            'mkt_cap': self.YF.get_market_cap(),
            'cur_price': self.YF.get_current_price(),
            'prev_price': self.YF.get_prev_close_price(),
            'beta': self.YF.get_beta(),
            'book_value': self.YF.get_book_value(),
            'sector': self.PF.sector,
            'eps': self.YF.get_earnings_per_share()
        }
        df = pd.DataFrame(data, index=[self.ticker])
        if not os.path.exists(self.info_dir):
            os.mkdir(self.info_dir)
        df.to_csv('{}/{}.csv'.format(self.info_dir, self.ticker), index=False)


def get_cross_sectional_data(ticker):
    try:
        yd = YahooData(ticker=ticker)
        yd.cache_cross_sectional_data()
    except Exception as e:
        print('couldnt get data for ticker: {}'.format(ticker))
        print('here is error: ', e)


def get_historical_price_data(ticker):
    try:
        yd = YahooData(ticker=ticker)
        yd.cache_price_data()
    except Exception as e:
        print('couldnt get price data for ticker: {}'.format(ticker))
        print('here is error: ', e)


if __name__ == '__main__':
    pool = mp.Pool(processes=mp.cpu_count())
    df = pd.read_csv('tickers.csv')
    total = len(df)
    args = list(df['tickers'])

    list(tqdm(pool.imap(get_cross_sectional_data, args), total=total))
    list(tqdm(pool.imap(get_historical_price_data, args), total=total))



