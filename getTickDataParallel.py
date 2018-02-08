from multiprocessing import Pool
import tushare as ts
import pdb, logging
from getTickData import _get_tick_data
from itertools import product
import datetime
import argparse
logging.basicConfig(level=logging.INFO)

from dateUtils import dateRange

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
        sdate = datetime.date(2018, 1, 1)
    # edate = datetime.date.today()
    edate = datetime.date(2018,2,4)
    allDates = dateRange(sdate, edate)
    
    pool = Pool()
    for ticker, date in product(allTickers, allDates):
        pool.apply_async(_get_tick_data, (ticker, date))

    pool.close()
    pool.join()
