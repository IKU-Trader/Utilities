# -*- coding: utf-8 -*-
"""
Created on Sun Dec  4 22:37:16 2022

@author: IKU-Trader
"""

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../TechnicalAnalysis'))

import pandas as pd
import numpy as np
import copy
from datetime import datetime, timedelta
from const import const
from TimeUtils import TimeUtils

from Utils import Utils
from STA import TechnicalAnalysis as ta
from MathArray import MathArray

class DataBuffer:
    # tohlcv: arrays ( time array, open array, ...)
    def __init__(self, tohlcv: list, ta_params: list, is_last_invalid=True):
        self.ta_params = ta_params
        dic, candle = self.tohlcvArrays2dic(tohlcv, is_last_invalid)        
        self.addIndicators(dic)
        self.dic = dic
        self.invalid_candle = candle
        
    def tohlcvArrays2dic(self, tohlcv: list, is_last_invalid):
        dic = {}
        if is_last_invalid:
            dic[const.TIME] = tohlcv[0][:-1]
            dic[const.OPEN] = tohlcv[1][:-1]
            dic[const.HIGH] = tohlcv[2][:-1]
            dic[const.LOW] = tohlcv[3][:-1]
            dic[const.CLOSE] = tohlcv[4][:-1]
            if len(tohlcv) > 5:
                dic[const.VOLUME] = tohlcv[5][:-1]
            candle = [tohlcv[0][-1], tohlcv[1][-1], tohlcv[2][-1],tohlcv[3][-1], tohlcv[4][-1]]
            if len(tohlcv) > 5:
                candle.append(tohlcv[5][-1])
            return dic, candle
        else:
            dic = self.arrays2Dic(tohlcv)
            return dic, []
        
    def arrays2Dic(self, tohlcvArrays: list):
        dic = {}
        dic[const.TIME] = tohlcvArrays[0]
        dic[const.OPEN] = tohlcvArrays[1]
        dic[const.HIGH] = tohlcvArrays[2]
        dic[const.LOW] = tohlcvArrays[3]
        dic[const.CLOSE] = tohlcvArrays[4]
        if len(tohlcvArrays) > 5:
            dic[const.VOLUME] = tohlcvArrays[5]
        return dic
                                
    def tohlcvDic(self):
        return self.dic
    
    def candles(self):
        return self.dic2Candles(self.dic)
    
    def tohlcvArrays(self):
        return Utils.dic2Arrays(self.dic)
    
    def size(self):
        return len(self.dic)
    
    def lastTime(self):
        if self.size() > 0:
            return self.dic[const.TIME][-1]
        else:
            return None
        
    def deltaTime(self):
        if self.size() > 1:
            time = self.dic[const.TIME]
            dt = time[1] - time[0]
            return dt
        else:
            return None
       
    # dic: tohlcv+ array dict
    def addIndicators(self, dic: dict):
        for ta_param in self.ta_params:
            method, param, name = ta_param
            ta.indicator(dic, method, param, name=name)
        return dic

    def updateSeqIndicator(self, dic: dict, begin: int, end: int):
        for name, [key, param] in self.ta_params.items():
            ta.seqIndicator(dic, key, begin, end, param, name=name)
        return dic
    
    # dic: tohlcv+ array dict
    def removeLastData(self, dic):
        keys, arrays = Utils.dic2Arrays(dic)
        out = {}
        for key, array in zip(keys, arrays):
            out[key] = array[:-1]
        return out
    
    # candles: tohlcv array
    def update(self, candles: list, is_last_invalid=True):
        if is_last_invalid:
            valid_candles = candles[:-1]
            self.invalid_candle = candles[-1]
        else:
            valid_candles = candles
            self.invalid_candle = None
        
        last_time = self.lastTime()
        new_candles = []
        for candle in valid_candles:
            if candle[0] > last_time:
                new_candles.append(candle)
        m = len(new_candles)
        if m > 0:
            begin = len(self.dic[const.TIME])        
            end = begin + m - 1
            self.merge(self.dic, new_candles)
            self.updateSeqIndicator(self.dic, begin, end)
           
    def temporary(self):
        if self.invalid_candle is None:
            return self.dic[const.TIME][-1], self.dic.copy()
        tmp_dic = copy.deepcopy(self.dic)
        self.merge(tmp_dic, [self.invalid_candle])        
        begin = len(tmp_dic[const.TIME]) - 1        
        end = begin
        self.updateSeqIndicator(tmp_dic, begin, end)
        return self.invalid_candle[0], tmp_dic

    def merge(self, dic: dict, candles: list):
        index = {const.TIME: 0, const.OPEN: 1, const.HIGH: 2, const.LOW: 3, const.CLOSE: 4, const.VOLUME: 5}
        n = len(candles)
        blank = MathArray.full(n, np.nan)
        for key, array in dic.items():
            try:
                i = index[key]
                a = [candle[i] for candle in candles]
                array += a
            except:
                array += blank.copy()
        return

    def arrays2Candles(self, tohlcvArrays: list):
        out = []
        n = len(tohlcvArrays[0])
        for i in range(n):
            d = [array[i] for array in tohlcvArrays]
            out.append(d)
        return out

    def candles2Arrays(self, candles: list):
        n = len(candles)
        m = len(candles[0])
        arrays = []
        for i in range(m):
            array = [candles[j][i] for j in range(n)]
            arrays.append(array)
        return arrays

    def dic2Candles(self, dic: dict):
        arrays = [dic[const.TIME], dic[const.OPEN], dic[const.HIGH], dic[const.LOW], dic[const.CLOSE]]
        try:
            arrays.append(dic[const.VOLUME])
        except:
            pass
        out = []
        for i in range(len(arrays[0])):
            d = [] 
            for array in arrays:
                d.append(array[i])
            out.append(d)
        return out
            
# -----

class ResampleDataBuffer(DataBuffer):
    # tohlcv: arrays ( time array, open array, ...)
    def __init__(self, tohlcv: list, ta_params: list, interval_minutes: int):
        tohlcv_arrays, tmp_candles = self.resample(tohlcv, interval_minutes, const.UNIT_MINUTE)
        super().__init__(tohlcv_arrays, ta_params, False)
        self.interval_minutes = interval_minutes
        self.tmp_candles = tmp_candles
            
    # candles: tohlcv array
    def update(self, candles):
        self.invalid_candle = candles[-1]
        valid_candles = candles[:-1]
        new_candles, tmp_candles = self.compositCandle(valid_candles)
        self.tmp_candles = tmp_candles
        m = len(new_candles)
        if m > 0:
            begin = len(self.dic[const.TIME])        
            end = begin + m - 1
            self.merge(self.dic, new_candles)
            self.updateSeqIndicator(self.dic, begin, end)
    
    def compositCandle(self, candles):
        tmp_candles = self.tmp_candles.copy()
        new_candles = []
        last_time = self.dic[const.TIME][-1]
        for candle  in candles:
            t = candle[0]
            if t <= last_time:
                continue
            t_round =  self.roundTime(t, self.interval_minutes, const.UNIT_MINUTE)
            if t == t_round:    
                tmp_candles.append(candle)
                c = self.candlePrice(t_round, tmp_candles)
                new_candles.append(c)
                tmp_candles = []
            else:
                if len(tmp_candles) > 0:
                    if t > tmp_candles[-1][0]:
                        tmp_candles.append(candle)
                else:
                    tmp_candles.append(candle)
        return new_candles, tmp_candles
        
    def temporary(self):
        tmp_candles = copy.deepcopy(self.tmp_candles)
        if self.invalid_candle is not None:
            tmp_candles.append(self.invalid_candle)
        if len(tmp_candles) == 0:
            return self.dic[const.TIME][-1], self.dic.copy()
        t = tmp_candles[-1][0]
        t_round =  self.roundTime(t, self.interval_minutes, const.UNIT_MINUTE)
        new_candle = self.candlePrice(t_round, tmp_candles)
        tmp_dic = copy.deepcopy(self.dic)
        begin = len(tmp_dic[const.TIME])
        self.merge(tmp_dic, [new_candle])        
        end = len(tmp_dic[const.TIME]) - 1
        self.updateSeqIndicator(tmp_dic, begin, end)
        return tmp_candles[-1][0], tmp_dic
    
    # tohlcv: tohlcv arrays
    def resample(self, tohlcv: list, interval: int, unit: const.TimeUnit):        
        time = tohlcv[0]
        n = len(time)
        op = tohlcv[1]
        hi = tohlcv[2]
        lo = tohlcv[3]
        cl = tohlcv[4]
        if len(tohlcv) > 5:
            vo = tohlcv[5]
            is_volume = True
        else:
            is_volume = False
        data_list = []
        candles = []
        current_time = None
        for i in range(n):
            t_round = self.roundTime(time[i], interval, unit)
            if is_volume:
                values = [time[i], op[i], hi[i], lo[i], cl[i], vo[i]]
            else:
                values = [time[i], op[i], hi[i], lo[i], cl[i]]
            if current_time is None:
                current_time = t_round
                data_list = [values]
            else:
                if t_round == current_time:
                    data_list.append(values)
                else:
                    candle = self.candlePrice(current_time, data_list)
                    candles.append(candle)
                    data_list = [values]
                    current_time = t_round
        if len(data_list) == interval:
            candle = self.candlePrice(current_time, data_list)
            candles.append(candle)
            data_list = []
        return self.candles2Arrays(candles), data_list
    
    def roundTime(self, time: datetime, interval: int, unit: const.TimeUnit):
        zone = time.tzinfo
        if unit == const.UNIT_MINUTE:
            t = datetime(time.year, time.month, time.day, time.hour, 0, 0, tzinfo=zone)
        elif unit == const.UNIT_HOUR:
            t = datetime(time.year, time.month, time.day, 0, 0, 0, tzinfo=zone)
        elif unit == const.UNIT_DAY:
            t = datetime(time.year, time.month, time.day, 0, 0, 0, tzinfo=zone)
            return t
        if t == time:
            return t
        while t < time:
            if unit == const.UNIT_MINUTE:
                t += timedelta(minutes=interval)
            elif unit == const.UNIT_HOUR:
                t += timedelta(hours=interval)
        return t

    def candlePrice(self, time:datetime, tohlcv_list:[]):
        m = len(tohlcv_list[0])
        n = len(tohlcv_list)
        o = tohlcv_list[0][1]
        c = tohlcv_list[-1][4]
        h_array = [tohlcv_list[i][2] for i in range(n)]
        h = max(h_array)
        l_array = [tohlcv_list[i][3] for i in range(n)]
        l = min(l_array)
        if m > 5:
            v_array = [tohlcv_list[i][5] for i in range(n)]
            v = sum(v_array)
            return [time, o, h, l, c, v]
        else:
            return [time, o, h, l, c]    