import datetime as dt
import os
import pandas as pd
from tqdm import tqdm
from multiprocessing import Manager, Process

class HistoricalPrices:
    def __init__(self, start_date=None, num_assets=967, num_days=None, prices_dir='prices/prices_{}'.format(dt.date.today().strftime('%Y-%m-%d'))):
        self.num_days = num_days
        self.num_assets = num_assets
        self.start_date = start_date
        self.prices_dir = prices_dir

    def get_prices(self):
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
        self.prices = prices

    def worker(self, d, filename):
        if filename.endswith('.csv'):
            ticker = filename.replace('.csv', '')
            fullpath = str('{}/{}'.format(self.prices_dir, filename))
            df = pd.read_csv(fullpath, engine='python')
            df['ticker'] = ticker
            d[ticker] = df

if __name__ == '__main__':
    hp = HistoricalPrices()
    hp.get_prices()
    print(hp.prices.head())
    tickers = hp.prices['ticker'].unique()
    print(len(tickers))


