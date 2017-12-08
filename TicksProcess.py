# -*- coding: utf-8 -*-
import pandas as pd
from gmsdk import *
import DATA_CONSTANTS as DC
#这个数据用的时候还要再洗一次：
#ticks数据中有些分钟数是交易时间外的，要拿1分钟的数据再做index筛选一次

contractlist=pd.read_excel(DC.PUBLIC_DATA_PATH+'Contract.xlsx')['Contract']

K_MIN=600
#K_MIN=600的情况下，部分合约（如CFFEX.T）不是从9：00开始的，处理起来会有错误，时间无法对齐
tradedatelist={
    'CFFEX':(pd.read_csv(DC.PUBLIC_DATA_PATH+'CFFEX tradedates.csv')),
    'CZCE':(pd.read_csv(DC.PUBLIC_DATA_PATH+'CZCE tradedates.csv')),
    'DCE':(pd.read_csv(DC.PUBLIC_DATA_PATH+'DCE tradedates.csv')),
    'SHFE':(pd.read_csv(DC.PUBLIC_DATA_PATH+'SHFE tradedates.csv'))
}

for symbol in contractlist:
    print symbol
    exchange_id,sec_id=symbol.split('.',1)
    tradedates = tradedatelist[exchange_id]
    tradedates.index = pd.to_datetime(tradedates['tradedate'])
    tradedate= tradedates.truncate(before=DC.TICKS_DATA_START_DATE)['tradedate']
    last_cumlong = 0
    last_cumshort = 0
    for oprdate in tradedate:
        filename=DC.TICKS_DATA_PATH+symbol+'\\'+symbol+oprdate+'ticks.csv'
        df=pd.read_csv(filename)
        df.index=pd.to_datetime(df['utc_time']-df['utc_time']%K_MIN,unit='s')
        df=df.tz_localize(tz='PRC')
        positiongroued=df['delta_position'].groupby(df.index)
        longgrouped=df['position_long'].groupby(df.index)
        shortgrouped=df['position_short'].groupby(df.index)
        delta_position=positiongroued.sum()
        position_long=longgrouped.sum()
        position_short=shortgrouped.sum()
        df2=pd.DataFrame({"delta_position":delta_position,"position_long":position_long,"position_short":position_short},index=positiongroued.groups)
        df2=df2.sort_index()

        df2['delta_longshort']=df2['position_long']-df2['position_short']
        cum_long=[]
        cum_short=[]
        cnum=df.shape[0]

        for i in position_long.index:
            last_cumlong+=position_long[i]
            last_cumshort+=position_short[i]
            cum_long.append(last_cumlong)
            cum_short.append(last_cumshort)

        df2['cum_long']=cum_long
        df2['cum_short']=cum_short
        df2['cum_delta']=df2['cum_long']-df2['cum_short']

        # 读取该品种分钟线数据，提取出时间index，用来过滤K线数据中的干扰数据
        rawdata = pd.read_csv(DC.RAW_DATA_PATH+symbol+'\\'+symbol+oprdate+' '+str(K_MIN)+'.csv')
        rawdata.index=pd.to_datetime(rawdata['utc_time'],unit='s')
        rawdata = rawdata.tz_localize(tz='PRC')
        df2=df2.loc[rawdata.index]

        df2.to_csv(DC.TICKS_DATA_PATH+symbol+'\\'+symbol+oprdate+'ticks ' + str(K_MIN)+'.csv')