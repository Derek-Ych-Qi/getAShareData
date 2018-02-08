import tushare as ts
import datetime
from itertools import product
import pdb, logging
import argparse
logging.basicConfig(level=logging.INFO)

from dateUtils import dateRange

__all_sources__ = ('sn', 'tt', 'nt')

def _get_tick_data(ticker, date):
    if not isinstance(date, str): date = str(date)
    logging.info('%s@%s' % (ticker, date))
    all_sources = list(__all_sources__)
    df = None
    while all_sources:
        try:
            src = all_sources.pop(0)
            df = ts.get_tick_data(ticker, date, src=src)
            if not df is None and df.shape[0] > 3: break
        except OSError:
            pass
    if df is None or df.shape[0] <= 3: raise IOError('No data from all sources for ticker %s at date %s' % (ticker, date))
    df.sort_values('time', inplace=True)
    df.reset_index(inplace=True, drop=True)
    df['date'] = date
    df['ticker'] = ticker
    df.to_hdf('data/hist_ticks.h5', key='%s/%s' % (date, ticker), mode='a')
    logging.info('%d' % df.shape[0])

def get_tick_data_serial_batch(allTickers, sdate=None, edate=datetime.date.today()):
    if not sdate: sdate = edate
    allDates = dateRange(sdate, edate)
    for ticker, date in product(allTickers, allDates):
        try:
            _get_tick_data(ticker, date)
        except Exception as e:
            nowStr = str(datetime.datetime.now())
            logging.exception(nowStr)

def get_tick_data_para_batch(allTickers, sdate, edate):
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', '-s', type=str)
    args = parser.parse_args()

    # allTickers = ts.get_hs300s()
    allTickers = ts.get_sz50s()
    allTickers = allTickers.code.tolist()
    if args.start:
        sdate = datetime.datetime.strptime(args.start, '%Y%m%d').date()
    else:
        sdate = datetime.date(2017, 1, 1)
    edate = datetime.date.today()
    get_tick_data_serial_batch(allTickers, sdate, edate)