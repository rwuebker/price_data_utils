import datetime as dt
import json
import numpy as np
import os
import pandas as pd
from pandas.tseries.offsets import BDay
from scipy.stats import zscore
from tqdm import tqdm


class DataLoader:

    def __init__(self, analysis_date_str=dt.date.today().strftime('%Y-%m-%d'),
                 prices_dir='prices', info_dir='info', periods=1, cached_dir='cached'):
        # prev date is decision date, trade date is the price we get
        self.cached_dir = cached_dir
        self.analysis_date_str = analysis_date_str
        self.analysis_date = dt.datetime.strptime(analysis_date_str, '%Y-%m-%d')
        self.periods = periods
        self.date_sets = {}
        self.cached = self.check_cached()

        self.pred_date = self.analysis_date + BDay(2)
        self.pred_date_str = self.pred_date.strftime('%Y-%m-%d')
        print('Prediction Date: {}'.format(self.pred_date_str))
        self.prev_date = self.analysis_date - BDay(periods)
        self.prev_date_str = self.prev_date.strftime('%Y-%m-%d')
        self.trade_date = self.analysis_date + BDay(1)
        self.trade_date_str = self.trade_date.strftime('%Y-%m-%d')
        self.year_ago = self.analysis_date - BDay(252)
        self.year_ago_str = self.year_ago.strftime('%Y-%m-%d')
        self.month_ago = self.analysis_date - BDay(22)
        self.month_ago_str = self.month_ago.strftime('%Y-%m-%d')
        self.prices_dir = prices_dir
        self.info_dir = info_dir

    def check_cached(self):
        cached_path = '{}/data/data_{}_periods_{}.csv'.format(self.cached_dir, self.analysis_date_str, self.periods)
        date_sets_path = '{}/date_sets/date_set_{}_periods_{}.json'.format(self.cached_dir, self.analysis_date_str, self.periods)
        if os.path.exists(cached_path) and os.path.exists(date_sets_path):
            df = pd.read_csv(cached_path, index_col=0)
            self.data = df
            self.data.index.names = ['ticker']

            with open(date_sets_path, 'r') as f:
                self.date_sets = json.load(f)
            return True
        return False

    def _cache_items(self):
        cached_path = '{}/data/data_{}_periods_{}.csv'.format(self.cached_dir, self.analysis_date_str, self.periods)
        date_sets_path = '{}/date_sets/date_set_{}_periods_{}.json'.format(self.cached_dir, self.analysis_date_str, self.periods)
        if not os.path.exists('{}/data'.format(self.cached_dir)):
            os.mkdirs('{}/data'.format(self.cached_dir))
        self.data.to_csv(cached_path)
        if not os.path.exists('{}/date_sets'.format(self.cached_dir)):
            os.mkdirs('{}/date_sets'.format(self.cached_dir))
        with open(date_sets_path, 'w') as f:
            json.dump(self.date_sets, f, indent=4, sort_keys=True, default=str)

    def load_data(self):
        if self.cached:
            return
        else:
            self._load_info()
            self._load_prices()
            self._cache_items()

    def _load_info(self):
        # use last business days date for info
        path = '{}/info_{}'.format(self.info_dir, self.analysis_date_str)
        totals = pd.DataFrame()
        if os.path.exists(path):
            for filename in os.listdir(path):
                if filename.endswith('.csv'):
                    df = pd.read_csv('{}/{}'.format(path, filename))
                    df.columns = ['ticker', 'mkt_cap', 'cur_price', 'prev_price', 'beta', 'book_value', 'sector', 'eps']
                    df.set_index('ticker', inplace=True)
                    df.drop(['cur_price', 'prev_price'], axis=1, inplace=True)
                    totals = totals.append(df)
        self.info = totals

    def _load_prices(self):
        path = '{}/prices_{}'.format(self.prices_dir, self.pred_date_str)
        info = self.info
        totals = pd.DataFrame()
        prices = pd.DataFrame()
        if os.path.exists(path):
            for filename in tqdm(os.listdir(path)):
                if filename.endswith('.csv'):
                    ticker = filename.replace('.csv', '')
                    self._initialize_date_set(ticker)
                    full_file_name = '{}/{}'.format(path, filename)
                    df = pd.read_csv(full_file_name, index_col='Date')
                    temp_df = self._load_price_specific_dates(df, ticker)
                    totals = totals.append(temp_df)
                    prices = prices.append(df)

            self.data = pd.merge(info, totals, left_index=True, right_index=True)
        else:
            print('path doesnt exist: ')
            print(path)

    def _load_price_specific_dates(self, df, ticker):
        data = {
                "month_ago": self._get_value(ticker, df, 'month_ago', 'Adj Close'),
                "year_ago": self._get_value(ticker, df, 'year_ago', 'Adj Close'),
                "prev_date": self._get_value(ticker, df, 'prev_date', 'Adj Close'),
                "analysis_date": self._get_value(ticker, df, 'analysis_date', 'Adj Close'),
                "trade_date": self._get_value(ticker, df, 'trade_date', 'Adj Close'),
                "pred_date": self._get_value(ticker, df, 'pred_date', 'Adj Close'),
                "volume": self._get_value(ticker, df, 'analysis_date', 'Volume')
        }
        return pd.DataFrame(data, index=[ticker])


    def _initialize_date_set(self, ticker):
        date_set = {
            'pred_date': self.pred_date,
            'pred_date_str': self.pred_date_str,
            'prev_date': self.prev_date,
            'prev_date_str': self.prev_date_str,
            'analysis_date': self.analysis_date,
            'analysis_date_str': self.analysis_date_str,
            'trade_date': self.trade_date,
            'trade_date_str': self.trade_date_str,
            'year_ago': self.year_ago,
            'year_ago_str': self.year_ago_str,
            'month_ago': self.month_ago,
            'month_ago_str': self.month_ago_str
        }
        self.date_sets[ticker] = date_set


    def _get_value(self, ticker, df, time_frame, value_name):
        counter = 0
        value = 'NOPE'
        while value == 'NOPE' and counter < 2:
            date_str = self.date_sets[ticker]['{}_str'.format(time_frame)]
            if date_str in df.index:
                value = df.at[date_str, value_name]
            else:
                value = 'NOPE'
                if time_frame == 'trade_date':
                    return np.nan
                self._adjust_date(ticker, time_frame)
                counter += 1
        if value == 'NOPE':
            return np.nan
        else:
            return value

    def _adjust_date(self, ticker, time_frame):
        if time_frame == 'trade_date':
            return False
        print('adjusting data for ticker: {} and this time frame: {}'.format(ticker, time_frame))
        self.date_sets[ticker][time_frame] = self.date_sets[ticker][time_frame] - BDay(1)
        self.date_sets[ticker]['{}_str'.format(time_frame)] = self.date_sets[ticker][time_frame].strftime('%Y-%m-%d')
        return True


if __name__ == '__main__':
    fa = DataLoader(analysis_date_str='2019-07-26')
    fa.load_data()
    print(fa.data.head())
