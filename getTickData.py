import tushare as ts
import datetime
import pdb, logging
import argparse
from multiprocessing import Pool
from itertools import product
logging.basicConfig(level=logging.INFO)

from dateUtils import tradingDateRange

_all_sources = ('nt', 'sn', 'tt')
_datadir = './data/tmp'

def _get_tick_data(ticker, date):
    if not isinstance(date, str): date = str(date)
    logging.info('%s@%s' % (ticker, date))
    df = None
    all_sources = list(_all_sources)
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
    # df.to_hdf('./data/hist_ticks.h5', key='%s/%s' % (date, ticker), mode='a')
    df.to_csv(_datadir + '/ticks_%s_%s.csv.gz' % (date, ticker), compression='gzip', index=False)
    logging.info('%d' % df.shape[0])

def get_tick_data_serial_batch(allTickers, sdate=None, edate=datetime.date.today()):
    if not sdate: sdate = edate
    allDates = tradingDateRange(sdate, edate)
    for ticker, date in product(allTickers, allDates):
        try:
            _get_tick_data(ticker, date)
        except Exception as e:
            nowStr = str(datetime.datetime.now())
            logging.exception(nowStr)

def get_tick_data_para_batch(allTickers, sdate=None, edate=datetime.date.today()):
    if not sdate: sdate = edate
    allDates = tradingDateRange(sdate, edate)
    pool = Pool()
    for ticker, date in product(allTickers, allDates):
        pool.apply_async(_get_tick_data, (ticker, date))
    pool.close()
    pool.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', '-s', type=str, default=None)
    parser.add_argument('--end',   '-e', type=str, default=None)
    parser.add_argument('--mode', '-m', type=str, default='S')
    args = parser.parse_args()

    allTickers = ts.get_hs300s()
    # allTickers = ts.get_sz50s()
    allTickers = allTickers.code.tolist()
    # Decide starting date
    if args.start: sdate = datetime.datetime.strptime(args.start, '%Y%m%d').date()
    else: sdate = None
    if args.end: edate = datetime.datetime.strptime(args.end, '%Y%m%d').date()
    else: edate = datetime.date.today()
    logging.info('Getting tick data from date %s to date %s' % (sdate, edate))
    # Decide mode
    if args.mode.upper() == 'P':
        get_tick_data_para_batch(allTickers, sdate, edate)
    else:
        get_tick_data_serial_batch(allTickers, sdate, edate)
