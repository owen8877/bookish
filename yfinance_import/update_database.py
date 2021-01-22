from datetime import date, timedelta, datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yfinance_import as yf
from dateutil.tz import tz

yf.pdr_override()


def download_clean_update(tickers, start, end, database_path, interval="5m"):
    # download dataframe
    _DFS = yf.multi.download(
        # tickers list or string as well
        tickers=tickers,

        # use "period" instead of start/end
        # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        # (optional, default is '1mo')
        start=start.strftime('%Y-%m-%d'),
        end=end.strftime('%Y-%m-%d'),

        # fetch data by interval (including intraday if period < 60 days)
        # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        # (optional, default is '1d')
        interval=interval,

        # group by ticker (to access via data['SPY'])
        # (optional, default is 'column')
        group_by='ticker',

        # adjust all OHLC automatically
        # (optional, default is False)
        auto_adjust=True,

        # download pre/post regular market hours data
        # (optional, default is False)
        prepost=True,

        # use threads for mass downloading? (True/False/Integer)
        # (optional, default is True)
        threads=True,

        # proxy URL scheme use use when downloading?
        # (optional, default is None)
        proxy=None
    )

    # filter trading hours
    opening_hour, opening_minute = 9, 30
    closing_hour, closing_minute = 16, 0

    # setup time indices
    time_index = []
    day = start
    if interval[-1] == 'm':
        increment = timedelta(minutes=int(interval[:-1]))
    else:
        increment = timedelta(hours=int(interval[:-1]))
    while day < end:
        time = datetime(day.year, day.month, day.day, opening_hour, opening_minute, tzinfo=tz.gettz('EST'))
        end_time = datetime(day.year, day.month, day.day, closing_hour, closing_minute, tzinfo=tz.gettz('EST'))
        while time < end_time:
            time_index.append(time)
            time += increment

        day += timedelta(days=1)

    time_index = pd.DatetimeIndex(time_index)
    for key in _DFS.keys():
        _DFS[key] = pd.DataFrame(index=time_index, data=_DFS[key]).dropna(axis=0, how='all')
    data = pd.concat(_DFS.values(), axis=1, keys=_DFS.keys())

    # clean and fill in nans
    data.interpolate(method="linear", inplace=True)
    data.fillna(method='pad', inplace=True)
    data.fillna(method='backfill', inplace=True)

    # dump to disk
    try:
        data_old: pd.DataFrame = pd.read_pickle(database_path)

        print('Old file exists.')
        old_index = set(data_old.index)
        insert_cond = np.logical_not(data.index.isin(old_index))

        data_new = data_old.append(data[insert_cond])
        data_new.to_pickle(database_path)
    except FileNotFoundError:
        print('Creating new file.')
        data.to_pickle(database_path)


def test_download_clean_update():
    database_path = 'data.test/5m.pickle'
    if Path(database_path).exists():
        Path(database_path).unlink()
    tickers = ('ARKK', 'AAPL', 'PLTR')

    plt.figure()
    start = date.today() - timedelta(days=10)
    end = date.today() - timedelta(days=5)
    download_clean_update(tickers=tickers, start=start, end=end, database_path=database_path)
    plt.subplot(2, 1, 1)
    data = pd.read_pickle(database_path)
    plt.plot(data[tickers[0]].Close, '.')

    start = date.today() - timedelta(days=4)
    end = date.today() - timedelta(days=1)
    download_clean_update(tickers=tickers, start=start, end=end, database_path=database_path)
    plt.subplot(2, 1, 2)
    data = pd.read_pickle(database_path)
    plt.plot(data[tickers[0]].Close, '.')

    plt.show()


def test_ticker_valid():
    database_path = f'data.test/valid.pickle'
    if Path(database_path).exists():
        Path(database_path).unlink()
    tickers = list(pd.read_csv('data.yahoo/symbols.csv').Symbol[:500])

    start = date.today() - timedelta(days=2)
    end = date.today() - timedelta(days=1)
    download_clean_update(tickers=tickers, start=start, end=end, database_path=database_path, interval="15m")


def download_history():
    database_path = 'data.yahoo/5m.pickle'
    tickers = list(pd.read_csv('data.yahoo/symbols.csv').Symbol)

    plt.figure()
    start = date.today() - timedelta(days=59)
    end = date.today()
    download_clean_update(tickers=tickers, start=start, end=end, database_path=database_path)


def download_day(day: date):
    database_path = 'data.yahoo/5m.pickle'
    tickers = {'ARKK', 'AAPL', 'PLTR'}

    plt.figure()
    download_clean_update(tickers=tickers, start=day, end=day, database_path=database_path)


if __name__ == '__main__':
    download_history()
