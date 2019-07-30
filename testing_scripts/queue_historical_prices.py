import os
import pandas as pd
from tqdm import tqdm
from multiprocessing import Manager, Process, Queue
import threading

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
        q = Queue()
        processes = []
        filenames = os.listdir(self.prices_dir)
        count = len(filenames)
        thread = threading.Thread(target=self.listener, args=(q, count))
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution

        for filename in filenames:
            p = Process(target=self.worker, args=(q, filename))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        q.push('kill')

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


