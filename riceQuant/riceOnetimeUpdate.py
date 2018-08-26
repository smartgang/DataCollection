# -*- coding: utf-8 -*-

import time
import pandas as pd
from datetime import datetime
import os


# 时间格式转换
def getEndutc(ricedf):
    # print ricedf['Unnamed: 0']
    # 有些的时间列没有index 的列名，要使用'Unnamed: 0'
    return int(time.mktime(time.strptime(ricedf['Unnamed: 0'], '%Y-%m-%d %H:%M:%S')))


def getEndutcfor1d(ricedf):
    # print ricedf['Unnamed: 0']
    # 有些的时间列没有index 的列名，要使用'Unnamed: 0'
    return int(time.mktime(time.strptime(ricedf['Unnamed: 0'] + ' 00:00:00', '%Y-%m-%d %H:%M:%S')))


def getstrtime(ricedf):
    return datetime.fromtimestamp(ricedf['utc_time'], tz=None).strftime('%Y-%m-%d %H:%M:%S') + '+08:00'


def getstrendtime(ricedf):
    return datetime.fromtimestamp(ricedf['utc_endtime'], tz=None).strftime('%Y-%m-%d %H:%M:%S') + '+08:00'


# 2018-04-02：米筐数据转换
def riceToMyquant1m(symbol, domain_symbol, enddate):
    rawfolder = ("riceToMyquant %s\\" % domain_symbol)
    bar_type = 60
    print('Processing %s of %s bartype%d enddate %s' % (symbol, domain_symbol, bar_type, enddate))
    filename = rawfolder + "%s_%s_1m.csv" % (symbol, enddate)
    exchange, sec = domain_symbol.split('.')
    ricedf = pd.read_csv(filename)
    ricedf['utc_endtime'] = ricedf.apply(lambda t: getEndutc(t), axis=1)
    ricedf['utc_time'] = ricedf['utc_endtime'] - bar_type
    ricedf['strtime'] = ricedf.apply(lambda t: getstrtime(t), axis=1)
    ricedf['strendtime'] = ricedf.apply(lambda t: getstrendtime(t), axis=1)
    myquantdf = pd.DataFrame()
    myquantdf['strtime'] = ricedf['strtime']
    myquantdf['strendtime'] = ricedf['strendtime']
    myquantdf['utc_time'] = ricedf['utc_time']
    myquantdf['utc_endtime'] = ricedf['utc_endtime']
    myquantdf['open'] = ricedf['open']
    myquantdf['high'] = ricedf['high']
    myquantdf['low'] = ricedf['low']
    myquantdf['close'] = ricedf['close']
    myquantdf['volume'] = ricedf['volume']
    myquantdf['amount'] = ricedf['total_turnover']
    myquantdf['position'] = ricedf['open_interest']
    myquantdf['limit_up'] = ricedf['limit_up']
    myquantdf['limit_down'] = ricedf['limit_down']
    myquantdf['adj_factor'] = 0
    myquantdf['pre_close'] = 0
    myquantdf['trading_date'] = ricedf['trading_date']
    myquantdf['exchange'] = exchange
    myquantdf['sec_id'] = sec
    myquantdf['symbol'] = symbol
    myquantdf['bar_type'] = bar_type
    tofilename = "%s %d_%s.csv" % (symbol, bar_type, enddate)
    myquantdf.to_csv("riceToMyquant " + domain_symbol + "\\" + tofilename, index=False)

if __name__ == '__main__':
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    symboldf = pd.read_csv('D:\\002 MakeLive\DataCollection\public data\\contractMap.csv')
    domain = 'SHFE.RB'
    enddate = '2018-06-19'
    symbol = 'RB1810'
    riceToMyquant1m(symbol, domain, enddate)

