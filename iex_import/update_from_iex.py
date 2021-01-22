import os
from datetime import date, timedelta
from pathlib import Path
from typing import List

import pandas as pd
from iexfinance.stocks import Stock
from tqdm import tqdm
from zipline.utils.tradingcalendar import get_trading_days


def update_range(tickers: List[str], start: date, end: date, directory: str = 'data.iex'):
    for ticker in tickers:
        prefix = f'{directory}/equity/minute/{ticker}'
        Path(prefix).mkdir(parents=True, exist_ok=True)

    for current in tqdm(get_trading_days(pd.Timestamp(start), pd.Timestamp(end)), desc='Trading day'):
        for ticker in tqdm(tickers, desc='Ticker'):
            security = Stock(ticker)
            prefix = f'{directory}/equity/minute/{ticker}'

            path = f'{prefix}/{current.strftime("%Y-%m-%d")}.p'
            if os.path.exists(path):
                # print(f'Minute data of {ticker} on {current.strftime("%Y-%m-%d")} exists, skip.')
                pass
            else:
                try:
                    intraday_prices = security.get_intraday_prices(exactDate=current.strftime('%Y%m%d'))
                    intraday_prices.reset_index(inplace=True)
                    intraday_prices.to_pickle(path)
                except Exception as e:
                    print(f'Caught error {e} when handling symbol {ticker}.')
                # print(f'Exported minute data of {ticker} on {current}.')


def test_update_range():
    os.environ['IEX_TOKEN'] = 'Tpk_1fb811ff94114185a66ad2ae390393ff'
    os.environ['IEX_API_VERSION'] = 'sandbox'
    os.environ['IEX_LOG_LEVEL'] = 'INFO'

    start = date.today() - timedelta(days=10)
    end = date.today() - timedelta(days=1)
    directory = 'data.test'

    symbols = pd.read_json(f'../data.iex/symbols.json')
    cs_etf_rows = symbols['type'].isin(('cs', 'et'))
    tickers = symbols[cs_etf_rows]['symbol'][:5]
    update_range(tickers=tickers, start=start, end=end, directory=directory)
    for ticker in tickers:
        prefix = f'{directory}/equity/minute/{ticker}'
        path = f'{prefix}/{start.strftime("%Y-%m-%d")}.p'
        print(pd.read_pickle(path).head())


def main(start: date, end: date):
    os.environ['IEX_TOKEN'] = 'pk_281682da77c64abdb9c9d902c1486187'
    os.environ['IEX_API_VERSION'] = 'stable'
    os.environ['IEX_LOG_LEVEL'] = 'INFO'

    directory = 'data.iex'
    symbols = pd.read_json(f'../data.iex/symbols.json')
    cs_etf_rows = symbols['type'].isin(('cs', 'et'))
    tickers = symbols[cs_etf_rows]['symbol']
    update_range(tickers=tickers, start=start, end=end, directory=directory)


if __name__ == '__main__':
    # test_update_range()
    main(start=date.today() - timedelta(days=10), end=date.today() - timedelta(days=1))
