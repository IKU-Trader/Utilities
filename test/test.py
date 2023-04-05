# -*- coding: utf-8 -*-
"""
Created on Wed Apr  5 07:47:06 2023

@author: IKU
"""

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../MarketData'))

import pandas as pd
from Converter import Converter
from MarketData import readFile, candles2tohlc, getCandles, str2time_fx
from const import const
from Utils import Utils
from TimeUtils import TimeUtils


def test_resample1():
    files = ['../../MarketData/GBPJPY/GBPJPY_202201/202201/GBPJPY_20220107.csv',
            '../../MarketData/GBPJPY/GBPJPY_202201/202201/GBPJPY_20220110.csv']
    
    candles = getCandles(files, str2time_fx)
    tohlc = candles2tohlc(candles)
    #Utils.saveArrays('./gbpjpy_1min.csv', tohlc)


    tohlc2, data = Converter.resample(tohlc, 15, const.UNIT_MINUTE)
    Utils.saveArrays('./gbpjpy_15min.csv', tohlc2)    
    
    
def test_resample2():
    df = pd.read_csv('./gbpjpy_15min.csv', index_col=0)

    op = list(df.iloc[:, 0].values)
    hi = list(df.iloc[:, 1].values)
    lo = list(df.iloc[:, 2].values)
    cl = list(df.iloc[:, 3].values)
    time = TimeUtils.str2pytimeArray(list(df.index), TimeUtils.TIMEZONE_TOKYO)
    tohlc = [time, op, hi, lo, cl]
    tohlc2, _ = Converter.resample(tohlc, 2, const.UNIT_HOUR)
    Utils.saveArrays('./gbpjpy_2hours.csv', tohlc2)


if __name__ == '__main__':
    test_resample2()