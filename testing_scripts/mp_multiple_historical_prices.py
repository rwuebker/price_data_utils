import os
import pandas as pd
from tqdm import tqdm, trange
from multiprocessing import Manager, Process, cpu_count

class HistoricalPrices:
    def __init__(self, start_date=None, num_assets=967, num_days=None, prices_dir='prices/prices_2019-07-26'):
        self.num_days = num_days
        self.num_assets = num_assets
        self.start_date = start_date
        self.prices_dir = prices_dir

    def get_prices(self):
        final_list = self.get_files()
        prices = pd.DataFrame()
        for df in tqdm(final_list, position=4):
            prices = prices.append(df, ignore_index=True)
        return prices

    def concat_dfs(self, prices_list, j, multiple):
        mgr = Manager()
        d = mgr.list()
        process_num = int(cpu_count() * multiple)
        list_len = len(prices_list)
        parts = int(list_len/process_num)
        jobs = []
        for i in trange(process_num, position=j):
            if i == (process_num - 1):
                df_list = prices_list[parts*i:]
                job = Process(target=self.worker_concat, args=(d, df_list, i,))
                job.start()
                jobs.append(job)
            else:
                df_list = prices_list[parts*i:(i+1)*parts]
                job = Process(target=self.worker_concat, args=(d, df_list, i,))
                job.start()
                jobs.append(job)

        for job in jobs:
            job.join()

        return d

    def get_files(self):
        mgr = Manager()
        l = mgr.list()
        filenames = os.listdir(self.prices_dir)
        count = len(filenames)
        jobs = []
        for filename in tqdm(filenames, position=1):
            job = Process(target=self.worker_read, args=(l, filename))
            job.start()
            jobs.append(job)

        for job in jobs:
            job.join()

        l = self.concat_dfs(l, 2, 8)
        l = self.concat_dfs(l, 3, 4)
        return l

    def worker_read(self, d, filename):
        if filename.endswith('.csv'):
            ticker = filename.replace('.csv', '')
            fullpath = str('{}/{}'.format(self.prices_dir, filename))
            df = pd.read_csv(fullpath, engine='python')
            df['ticker'] = ticker
            d.append(df)

    def worker_concat(self, d, df_list, i):
        prices = pd.DataFrame()
        for df in df_list:
            prices = prices.append(df, ignore_index=True)
        d.append(prices)

if __name__ == '__main__':
    hp = HistoricalPrices()
    prices = hp.get_prices()
    print(prices.head())
    tickers = prices['ticker'].unique()
    print(len(tickers))


