import os
import pandas as pd
from tqdm import tqdm
from multiprocessing import Manager, Process

class HistoricalPrices:
    def __init__(self, start_date=None, num_assets=967, num_days=None, prices_dir='prices/prices_2019-07-26'):
        self.num_days = num_days
        self.num_assets = num_assets
        self.start_date = start_date
        self.prices_dir = prices_dir

    def get_prices(self):
        mgr = Manager()
        ns = mgr.Namespace()
        ns.prices = pd.DataFrame()
        d = mgr.dict()
        filenames = os.listdir(self.prices_dir)
        count = len(filenames)
        jobs = []
        for filename in tqdm(filenames):
            job = Process(target=self.worker, args=(ns, filename))
            job.start()
            jobs.append(job)

        for job in jobs:
            job.join()

        prices = ns.prices

        #print(prices.head())
        self.prices = prices

    def worker(self, ns, filename):
        if filename.endswith('.csv'):
            ticker = filename.replace('.csv', '')
            fullpath = str('{}/{}'.format(self.prices_dir, filename))
            df = pd.read_csv(fullpath, engine='python')
            df['ticker'] = ticker
            while True:
                try:
                    ns.prices = pd.concat([ns.prices, df], ignore_index=True)
                    break
                except Exception as e:
                    pass

if __name__ == '__main__':
    hp = HistoricalPrices()
    hp.get_prices()
    print(hp.prices.head())
    tickers = hp.prices['ticker'].unique()
    print(len(tickers))


