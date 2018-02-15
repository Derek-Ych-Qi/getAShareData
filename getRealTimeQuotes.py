import tushare as ts
import datetime
import numpy as np
import pandas as pd
import pdb, logging
import time
import argparse
from multiprocessing import Pool
from itertools import cycle

_mstart = datetime.time(9,10,0)
_mend   = datetime.time(11,40,0)
_astart = datetime.time(12,50,0)
_aend   = datetime.time(15,10,0)

datadir = './data/tmp'
today = datetime.date.today()

def _checkTime(t):
    if t.time() > _mstart and t.time() < _mend:
        return True
    elif t.time() > _astart and t.time() < _aend:
        return True
    else:
        return False

class QuoteGetter(object):
    def __init__(self, tickers):
        self.tickers = tickers
        self.fileName = datadir + '/quotes_%s_%s.csv.gz' % (today, 'sz50') #XXX
        self.fileHandle = open(self.fileName, 'w')

    def getQuoteSnapshot(self):
        quotes = ts.get_realtime_quotes(self.tickers)
        quotes.to_csv(self.fileHandle, mode='a', compression='gzip', header=False)
        
    def saveHeader(self):
        header = ts.get_realtime_quotes(self.tickers)
        header.to_csv(self.fileHandle, compression='gzip')    

    def __del__(self):
        self.fileHandle.close()

if __name__ == "__main__":
    allTickers = ts.get_sz50s()
    allTickers = allTickers.code.tolist()
    qg = QuoteGetter(tickers=allTickers)
    # while(_checkTime(datetime.datetime.now())):
    qg.saveHeader()

    for i in range(10):
        try:
            qg.getQuoteSnapshot()
        except Exception as e:
            logging.exception(e.__repr__())
