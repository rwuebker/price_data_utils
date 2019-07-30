import os
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool
import multiprocessing as mp
import shutil


def HistoricalPrices(prices_dir='prices/prices_2019-07-26'):
    directory = 'tmp/stock_data'
    if os.path.exists(directory):
        shutil.rmtree('tmp/stock_data')
    if not os.path.exists(directory):
        os.makedirs(directory)
    processes = mp.cpu_count()
    pool = mp.Pool(processes=processes)
    files = os.listdir(prices_dir)
    total = len(files)
    part = int(total/processes)
    args = []
    for i in range(processes):
        if i == (processes - 1):
            filenames = files[i*part:]
        else:
            filenames = files[i*part:(i+1)*part]
        args.append((filenames, i))

    pool.starmap(_post_df, args)
    prices = pd.DataFrame()
    for filename in tqdm(os.listdir(directory), position=8, desc='Combining Process'):
        fullpath = '{}/{}'.format(directory, filename)
        df = pd.read_csv(fullpath)
        prices = prices.append(df)

    prices.set_index(['ticker', 'Date'], inplace=True)
    return prices

def _post_df(filenames, i):
    prices = pd.DataFrame()
    for f in tqdm(filenames, position=i, desc='Process {}'.format(i)):
        if f.endswith('csv'):
            ticker = f.replace('.csv', '')
            fullpath = str('{}/{}'.format('prices/prices_2019-07-26', f))
            df = pd.read_csv(fullpath, engine='python')
            df['ticker'] = ticker
            prices = prices.append(df)
    directory = 'tmp/stock_data'
    prices.to_csv('{}/{}.csv'.format(directory, ticker), index=False)




if __name__ == '__main__':
    prices = HistoricalPrices()
    print(prices.head())
    tickers = len(prices.groupby(level=0))
    print('this is ticker count: ', tickers)



