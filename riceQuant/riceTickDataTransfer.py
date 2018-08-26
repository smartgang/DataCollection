# -*- coding: utf-8 -*-
"""
以日为单位，转换清洗米筐tick数据
"""
import time
import pandas as pd
from datetime import datetime
import os


# 时间格式转换
def getEndutc(ricedf):
    # print ricedf['Unnamed: 0']
    # 有些的时间列没有index 的列名，要使用'Unnamed: 0'
    return int(time.mktime(time.strptime(ricedf['datetime_sec'], '%Y-%m-%d %H:%M:%S')))


def getEndutcfor1d(ricedf):
    # print ricedf['Unnamed: 0']
    # 有些的时间列没有index 的列名，要使用'Unnamed: 0'
    return int(time.mktime(time.strptime(ricedf['Unnamed: 0'] + ' 00:00:00', '%Y-%m-%d %H:%M:%S')))


def getstrtime(ricedf):
    return datetime.fromtimestamp(ricedf['utc_time'], tz=None).strftime('%Y-%m-%d %H:%M:%S') + '+08:00'


def getstrendtime(ricedf):
    return datetime.fromtimestamp(ricedf['utc_endtime'], tz=None).strftime('%Y-%m-%d %H:%M:%S') + '+08:00'


def riceToMyquantTick(ricedf):
    a = ricedf['datetime'].str.partition('.')
    ricedf.loc[:, 'datetime_sec'] = a[0]
    ricedf.loc[:, 'datetime_ms'] = a[2]
    ricedf.loc[:, 'utc_time'] = ricedf.apply(lambda t: getEndutc(t), axis=1)
    ricedf.loc[:, 'utc_time_full'] = ricedf['utc_time'] + ricedf['datetime_ms'].astype('int')/1000
    return ricedf


if __name__=="__main__":
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    domain_symbol = 'SHFE.RB'
    symbol = 'RB1810'
    tick_data_folder = "rqdata_raw_%s\\raw_tick_%s\\" % (domain_symbol, symbol)
    file_list = os.listdir(tick_data_folder)
    for f1 in file_list:
        print f1
        df = pd.read_csv(tick_data_folder + f1)
        new_df = riceToMyquantTick(df)
        to_f1 = f1.replace('raw_tick', 'Tick_Data')
        new_df.to_csv(tick_data_folder + to_f1, index=False)
