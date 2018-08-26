# -*- coding: utf-8 -*-
'''
目标：将tick级数据中的成交量和持仓量映射到分钟级的数据中
指标：
持仓量
多方持仓量：position_long累加
空方持仓量：position_short累加
多空持仓量差值：多方持仓量-空方持仓量
成交量
成交量总额：volume_delta=cum_volume(n)-cum_volume(n-1)
总时间（数量）：tick数量
多方成交量：volume_delta(trade_type=3/6)+volume_delta(trade_type==1/2)/2
多方时间：trade_type==3/6的tick数量+trade_type==1/2的tick数量/2
空方成交量：volume_delta(trade_type=4/5)+volume_delta(trade_type==1/2)/2
空方时间：trade_type==4/5的tick数量+trade_type==1/2的tick数量/2
多方大单成交量：多方行情下 volume_delta大于 大单额
多方大单时间（数量）
空方大单成交量：空方行情下 volume_delta大于 大单额
空方大单时间（数量）
代码流程：
计算持仓量
需要保留上一日最后的多方持仓量、空方持仓量、差值
计算volulme_delta：
cum_volume(n)-cum_volume(n-1)
需要保留上一日最后的取值
对时间取整：utc_time-mod(utc_time,60)
以utc_time和trade_type去groupby
从groupby结果中计算各个取值，以utc_time进行汇总
将结果汇整对齐到分钟数据中
注：
先验证一下trade_type的识别是否正确
'''
import pandas as pd
import DATA_CONSTANTS as DC
import os

def volumeProcess(tickdata,last_cum_position,last_cum_position_long,last_cum_position_short,last_delta_volume):
    '''
    tick数据交易量和持仓量处理
    :param tickdata:
    :param last_position_long:
    :param last_position_short:
    :param last_delta_volume:
    :return:
    '''
    #原始数据中对delta_position的处理有问题，position_long和position_short有错，重新计算
    tickdata['delta_position'] = tickdata['cum_position'] - tickdata['cum_position'].shift(1).fillna(0)
    tickdata.ix[0, 'delta_position'] -= last_cum_position
    # volume数据是每日重新计算的，所以不用加上上一日的结束值
    tickdata['delta_volume'] = tickdata['cum_volume'] - tickdata['cum_volume'].shift(1).fillna(0)
    # 因为shift1第一个填充为0，所以要减掉上一个结束值
    tickdata.ix[0, 'delta_volume'] -= last_delta_volume
    tickdata.loc[tickdata['delta_volume']<0,'delta_volume']=tickdata['cum_volume']

    tickdata['utc_time_1m'] = tickdata['utc_time'] - tickdata['utc_time'].mod(60)
    # 处理全量数据
    time_trade_group = tickdata.groupby(['utc_time_1m', 'trade_type'])
    volume_delta = time_trade_group['delta_volume'].sum().unstack().fillna(0)
    volume_delta_count = time_trade_group['delta_volume'].count().unstack().fillna(0)

    # 处理大单数据
    time_trade_big_group = tickdata.loc[tickdata['delta_volume'] > 500].groupby(['utc_time_1m', 'trade_type'])
    volume_delta_big = time_trade_big_group['delta_volume'].sum().unstack().fillna(0)
    volume_delta_big_count = time_trade_big_group['delta_volume'].count().unstack().fillna(0)

    # print volume_delta
    result = pd.DataFrame()
    #持仓数据处理
    position_delta = time_trade_group['delta_position'].sum().unstack().fillna(0)
    result['position_long'] = position_delta[3] + position_delta[5] + (position_delta[1] + position_delta[2]) / 2
    result['position_short'] = position_delta[4] + position_delta[6] + (position_delta[1] + position_delta[2]) / 2
    #这里要加上前一日的终值
    result['cum_position_long']=result['position_long'].expanding().sum().fillna(0)
    result['cum_position_short']=result['position_short'].expanding().sum().fillna(0)
    result['cum_position_long']+=last_cum_position_long
    result['cum_position_short']+=last_cum_position_short
    result['delta_position'] = result['cum_position_long']-result['cum_position_short']

    result['volume_delta'] = volume_delta.sum(axis=1)
    result['volume_delta_count'] = volume_delta_count.sum(axis=1)
    result['volume_delta_long'] = volume_delta[3] + volume_delta[5] + (volume_delta[1] + volume_delta[2]) / 2
    result['volume_delta_long_count'] = volume_delta_count[3] + volume_delta_count[5] + (volume_delta_count[1] +
                                                                                         volume_delta_count[2]) / 2
    result['volume_delta_short'] = volume_delta[4] + volume_delta[6] + (volume_delta[1] + volume_delta[2]) / 2
    result['volume_delta_short_count'] = volume_delta_count[4] + volume_delta_count[6] + (volume_delta_count[1] +
                                                                                          volume_delta_count[2]) / 2

    result['volume_big_delta_long'] = volume_delta_big[3] + volume_delta_big[5]
    result['volume_big_delta_long_count'] = volume_delta_big_count[3] + volume_delta_big_count[5]
    result['volume_big_delta_short'] = volume_delta_big[4] + volume_delta_big[6]
    result['volume_big_delta_short_count'] = volume_delta_big_count[4] + volume_delta_big_count[6]

    #time_group = tickdata.groupby(['utc_time_1m'])
    #result['position_long'] = time_group['cum_position_long'].last()
    #result['position_short'] = time_group['cum_position_short'].last()
    #result['delta_position'] = time_group['delta_position'].last()
    result.fillna(0, inplace=True)

    last_cum_position = tickdata['cum_position'].iloc[-1]
    last_cum_volume = tickdata['cum_volume'].iloc[-1]
    last_cum_position_long = result['cum_position_long'].iloc[-1]
    last_cum_position_short = result['cum_position_short'].iloc[-1]

    return result,last_cum_position,last_cum_position_long,last_cum_position_short,last_cum_volume

def volumeProcess1m(symbol,startdate,enddate):
    '''
    将tick数据中的交易量和持仓量数据转换成1m级别,保存到volume data文件夹中
    :param symbol:
    :param startdate:
    :param enddate:
    :return:
    '''
    #读取tick数据
    exchange_id,sec_id=symbol.split('.',1)
    tradedates=DC.getTradedates(exchange_id,startdate,enddate)['strtime']
    firstdayTick = DC.getTickByDate(symbol, startdate)
    last_delta_position=firstdayTick['cum_position'].iloc[0]
    last_cum_position_long=0
    last_cum_position_short=0
    last_delta_volume=firstdayTick['cum_volume'].iloc[0]
    results = pd.DataFrame()
    #1.先将tick数据处理
    for td in tradedates:
        print ("process tick data of %s %s"%(symbol,td))
        tickdata=DC.getTickByDate(symbol,td)
        result,last_delta_position,last_cum_position_long,last_cum_position_short,last_delta_volume=\
            volumeProcess(tickdata,last_delta_position,last_cum_position_long,last_cum_position_short,last_delta_volume)
        results = pd.concat([results, result])
    #2.取1m数据进行对齐
    bardata=DC.getBarData(symbol=symbol,K_MIN=60,starttime=startdate+' 00:00:00',endtime=enddate+' 23:59:59')
    bardata.set_index('utc_time',inplace=True)

    results=results.loc[bardata.index]
    results['bar_type']=bardata['bar_type']
    results['exchange'] = bardata['exchange']
    results['sec_id'] = bardata['sec_id']
    results['strtime'] = bardata['strtime']
    results['strendtime'] = bardata['strendtime']
    results['utc_endtime'] = bardata['utc_endtime']
    results['close'] = bardata['close']
    results.reset_index(drop=False,inplace=True)
    os.chdir(DC.VOLUME_DATA_PATH)
    try:
        os.mkdir(symbol)
    except:
        print ("%s folder already exist!")
    results.to_csv("\\%s\\%s %d_volume.csv"%(symbol,symbol,60))
    print ("%s tick volume processed from %s to %s!"%(symbol,startdate,enddate))

def volumeProcessKMIN(symbol,K_MIN,startdate,enddate):
    '''
    多分钟周期的tick数据转换
    采用对齐、填充、groupby的处理方式
    先取时间区间的KMIN数据，将utc_time设为index
    取相同时间区间的60_volume数据，将utc_time设为index，两边对齐，向下填充（与上一个非空值相同）为utc_time
    使用填充后的utc_time列作为key值，进行groupby，各列在groupby取值如下
    utc_time：第1个值
    position_long：累加
    position_short：累加
    cum_position_long：最后值
    cum_position_short：最后值
    delta_position：最后值
    volume_delta：累加
    volume_delta_count：累加
    volume_delta_long：累加
    volume_delta_long_count：累加
    volume_delta_short：累加
    volume_delta_short_count：累加
    volume_big_delta_long：累加
    volume_big_delta_long_count：累加
    volume_big_delta_short：累加
    volume_big_delta_short_count：累加
    bar_type：KMIN取值
    exchange
    sec_id
    strtime：生成后再直接从bar数据取值
    strendtime：生成后再直接从bar数据取值
    utc_endtime：生成后再直接从bar数据取值
    close：结束值

    :param symbol:
    :param K_MIN:
    :param startdate:
    :param enddate:
    :return:
    '''
    volume1m=DC.getVolumeData(symbol=symbol,K_MIN=60,starttime=startdate+' 00:00:00',endtime=enddate+' 23:59:59')
    volume1m.set_index('utc_time',inplace=True)
    bardata=DC.getBarData(symbol=symbol,K_MIN=K_MIN,starttime=startdate+' 00:00:00',endtime=enddate+' 23:59:59')
    bardata.set_index('utc_time',drop=False,inplace=True)
    volume1m['KMIN_utc']=bardata['utc_time']
    volume1m['KMIN_utc'].fillna(method='ffill',inplace=True)
    #print volume1m.head(100)
    group1m=volume1m.groupby('KMIN_utc')
    result=pd.DataFrame()
    result['position_long']=group1m['position_long'].sum()
    result['position_short']=group1m['position_short'].sum()
    result['cum_position_long']=group1m['cum_position_long'].last()
    result['cum_position_short']=group1m['cum_position_short'].last()
    result['delta_position']=group1m['delta_position'].last()#最后值
    result['volume_delta']=group1m['volume_delta'].sum()#累加
    result['volume_delta_count']=group1m['volume_delta_count'].sum()#累加
    result['volume_delta_long']=group1m['volume_delta_long'].sum()#累加
    result['volume_delta_long_count']=group1m['volume_delta_long_count'].sum()#累加
    result['volume_delta_short']=group1m['volume_delta_short'].sum()#累加
    result['volume_delta_short_count']=group1m['volume_delta_short_count'].sum()#累加
    result['volume_big_delta_long']=group1m['volume_big_delta_long'].sum()#累加
    result['volume_big_delta_long_count']=group1m['volume_big_delta_long_count'].sum()#累加
    result['volume_big_delta_short']=group1m['volume_big_delta_short'].sum()#累加
    result['volume_big_delta_short_count']=group1m['volume_big_delta_short_count'].sum()#累加
    result['utc_time']=bardata['utc_time']
    result['bar_type']=bardata['bar_type']
    result['exchange']=bardata['exchange']
    result['sec_id']=bardata['sec_id']
    result['strtime']=bardata['strtime']#生成后再直接从bar数据取值
    result['strendtime']=bardata['strendtime']#生成后再直接从bar数据取值
    result['utc_endtime']=bardata['utc_endtime']#生成后再直接从bar数据取值
    result['close']=group1m['close'].last()
    os.chdir(DC.VOLUME_DATA_PATH)
    try:
        os.mkdir(symbol)
    except:
        print ("%s folder already exist!")
    result.to_csv("%s\\%s %d_volume.csv"%(symbol,symbol,K_MIN))
    print ("%s %d volume processed from %s to %s!"%(symbol,K_MIN,startdate,enddate))
    pass

if __name__=='__main__':
    K_MIN_list=[300,900,1800,3600,7200]
    #volumeProcess1m('SHFE.RB','2017-04-05','2018-03-30')
    for KMIN in K_MIN_list:
        volumeProcessKMIN('SHFE.RB',KMIN,'2017-04-05','2018-03-30')
    #results.to_csv('results60.csv')