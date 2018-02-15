import numpy as np
import pandas as pd
import os, glob, logging, datetime

_tmpDir = './data/tmp/'

def parseFileName(fileName):
    fileName = fileName.split(_tmpDir)[1]
    groups = fileName.split('_')
    field, dateStr, ticker = groups[0], groups[1], groups[2][:6]
    return field, dateStr, ticker

def saveFile(fileName):
    df = pd.read_csv(fileName)
    field, dateStr, ticker = parseFileName(fileName)
    destination = './data/%s.h5' % field
    key = '%s/%s' % (dateStr, ticker)
    df.to_hdf(destination, key=key, mode='a')

def serializeData():
    dataRoot = './data/tmp'
    for fileName in os.listdir(dataRoot):
        logging.info(fileName)
        try:
            saveFile(os.path.join(dataRoot, fileName))
        except Exception as e:
            logging.warning('%s:Serialization failed for file %s\n%s' % (datetime.datetime.now(), fileName, e.__repr__()))

if __name__ == "__main__":
    serializeData()
