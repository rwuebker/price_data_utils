import datetime as dt
import os
import pandas as pd
from tqdm import tqdm
from multiprocessing import Manager, Process

class HistoricalPrices:
    def __init__(self, prices_dir='prices', date_str=dt.date.today().strftime('%Y-%m-%d'),
                 cached_dir='cached'):
        self.date_str = date_str
        self.prices_dir = '{}/prices_{}'.format(prices_dir, date_str)
        self.cached_dir = cached_dir
        self.cached = self.check_cached()


    def check_cached(self):
        cached_path = '{}/prices/prices_{}.csv'.format(self.cached_dir, self.date_str)
        if os.path.exists(cached_path):
            self.prices = pd.read_csv(cached_path, index_col='Date')
            return True
        return False


    def get_prices(self):
        if self.cached:
            return
        else:
            mgr = Manager()
            d = mgr.dict()
            filenames = os.listdir(self.prices_dir)
            count = len(filenames)
            jobs = []
            for filename in tqdm(filenames, position=1):
                job = Process(target=self.worker, args=(d, filename))
                job.start()
                jobs.append(job)

            for job in jobs:
                job.join()

            prices = pd.concat(d.values(), ignore_index=True)
            self.prices = prices.set_index('Date')
            self.cache_prices()
            self.cached = True

    def worker(self, d, filename):
        if filename.endswith('.csv'):
            ticker = filename.replace('.csv', '')
            fullpath = str('{}/{}'.format(self.prices_dir, filename))
            df = pd.read_csv(fullpath, engine='python')
            df['ticker'] = ticker
            d[ticker] = df

    def cache_prices(self):
        cached_path = '{}/prices/prices_{}.csv'.format(self.cached_dir, self.date_str)
        if not os.path.exists('{}/prices'.format(self.cached_dir)):
            os.mkdirs('{}/prices'.format(self.cached_dir))
        prices = self.prices.reset_index()
        prices.to_csv(cached_path, index=False)

if __name__ == '__main__':
    hp = HistoricalPrices()
    hp.get_prices()
    print(hp.prices.head())
    tickers = hp.prices['ticker'].unique()
    print(len(tickers))


