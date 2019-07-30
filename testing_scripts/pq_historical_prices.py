import os
import pandas as pd
from tqdm import tqdm
from multiprocessing import Manager, Process, Queue
import multiprocessing as mp

class HistoricalPrices:
    def __init__(self, start_date=None, num_assets=967, num_days=None, prices_dir='prices/prices_2019-07-26'):
        self.num_days = num_days
        self.num_assets = num_assets
        self.start_date = start_date
        self.prices_dir = prices_dir
        self.prices = pd.DataFrame()

    def listener(self, q, count):
        with tqdm(total=count) as pbar:
            while True:
                df = q.get()
                if type(df) == str:
                    break
                self.prices = self.prices.append(df)
                pbar.update()

    def get_prices(self):
        mgr = Manager()
        q = mgr.Queue()
        filenames = os.listdir(self.prices_dir)
        count = len(filenames)
        pool = mp.Pool(count)
        watcher = pool.apply_async(self.listener, (q,count,))

        jobs = []
        for filename in filenames:
            job = pool.apply_async(self.worker, (q, filename,))
            jobs.append(job)

        for job in jobs:
            job.get()

        q.put('kill')

        return self.prices

    def worker(self, q, filename):
        if filename.endswith('.csv'):
            ticker = filename.replace('.csv', '')
            fullpath = str('{}/{}'.format(self.prices_dir, filename))
            df = pd.read_csv(fullpath, engine='python')
            df['ticker'] = ticker
            q.put(df)


if __name__ == '__main__':
    hp = HistoricalPrices()
    hp.get_prices()


