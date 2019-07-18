#!/usr/bin/env python
import datetime as dt
import multiprocessing as mp
import pandas as pd
from tqdm import tqdm
from yahoofinancials import YahooFinancials as yf

class YahooData:

    def __init__(self, tickers=['AAPL', 'MSFT', 'AYI'], name=None, start_date=None, end_date=None):
        if end_date is None:
            end_date = dt.date.today().strftime('%Y-%m-%d')
        self.start_date = start_date
        self.end_date = end_date
        self.tickers = tickers
        self.YF = yf(self.tickers)
        self.name = name

    def get_price_data(self, start=None, end=None):

        all_prices = self.YF.get_historical_price_data(self.start_date, self.end_date, 'daily')
        prices_df = pd.DataFrame()
        for key, val in all_prices.items():
            prices = all_prices[key]['prices']
            df = pd.DataFrame(prices)
            df['ticker'] = key
            columns = ['high', 'low', 'adjclose', 'volume']
            df = pd.DataFrame(df[columns].values, index=([df['ticker'].values, df['formatted_date'].values]), columns=columns)
            prices_df = prices_df.append(df)
        prices_df.index.names = ['ticker', 'date']
        self.historical_prices = prices_df
        return prices_df
        # comments for multiindex slicing:
        #print(prices_df.index.names)
        #print(prices_df.loc[(slice(None), '2018-12-27'),:])
        #print(prices_df.index.get_level_values(0))

    def get_cross_sectional_data(self):
        mkt_cap = self.YF.get_market_cap()
        cur_prices = self.YF.get_current_price()
        prev_prices = self.YF.get_prev_close_price()
        beta = self.YF.get_beta()
        book_value = self.YF.get_book_value()

        mc_df = pd.DataFrame(mkt_cap, index=['mkt_cap'])
        df = mc_df.append(pd.DataFrame(cur_prices, index=['cur_price']))
        df = df.append(pd.DataFrame(prev_prices, index=['prev_price']))
        df = df.append(pd.DataFrame(beta, index=['beta']))
        df = df.append(pd.DataFrame(book_value, index=['book_value'])).transpose()
        df.to_csv('stocks/{}'.format(self.name))

def my_func(tickers):
    name = '{}.csv'.format(tickers[0])
    try:
        yd = YahooData(tickers=tickers, name=name)
        yd.get_cross_sectional_data()
    except Exception as e:
        print('couldnt get data for ticker: {}'.format(tickers[0]))
        print('here is error: ', e)

if __name__ == '__main__':
    pool = mp.Pool(processes=8)
    df = pd.read_csv('tickers.csv')
    #yd = YahooData(tickers=df['tickers'].values)
    #yd = YahooData()
    args = [list(v) for v in df.values]
    list(tqdm(pool.imap(my_func, args), total=1000))






