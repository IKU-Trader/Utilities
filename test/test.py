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

def test_resample():
    files = ['../MarketData/GBPJPY/GBPJPY_202201/202201_tmp/GBPJPY_20220107.csv',
            '../MarketData/GBPJPY/GBPJPY_202201/202201_tmp/GBPJPY_20220110.csv']
    
    candles = getCandles(files, str2time_fx)
    tohlc = candles2tohlc(candles)

    tohlc2, data = Converter.resample(tohlc, 2, const.UNIT_HOUR)
    Utils.saveArrays('./gbpjpy_2hour.csv', tohlc2)    
    
    
    pass



if __name__ == '__main__':
    test_resample()