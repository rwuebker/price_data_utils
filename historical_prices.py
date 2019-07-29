import os
import pandas as pd
from tqdm import tqdm

class HistoricalPrices:
    def __init__(self, start_date=None, num_assets=967, num_days=None, prices_dir='prices/prices_2019-07-26'):
        self.num_days = num_days
        self.num_assets = num_assets
        self.start_date = start_date
        self.prices_dir = prices_dir

    def get_prices(self):
        prices = pd.DataFrame()
        for filename in tqdm(os.listdir(self.prices_dir)):
            if filename.endswith('.csv'):
                ticker = filename.replace('.csv', '')
                fullpath = str('{}/{}'.format(self.prices_dir, filename))
                df = pd.read_csv(fullpath, engine='python')
                df['ticker'] = ticker
                prices = prices.append(df)

        prices.set_index(['ticker', 'Date'], inplace=True)
        self.prices = prices


if __name__ == '__main__':
    hp = HistoricalPrices()
    hp.get_prices()
    print(hp.prices.head())


