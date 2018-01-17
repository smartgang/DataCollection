# -*- coding: utf-8 -*-
'''
将ricequant的数据，转换为myquant
不变：
    open
    high
    low
    close
    volume
更名：
    total_turnover->amount
    open_interest->position
增加：
    bar_type
    exchange
    sec_id
    strtime:2016-01-04T09:00:00+08:00
    strendtime:2016-01-04T09:00:00+08:00
    utc_time:1451869140
    utc_endtime:451869200`
    Unnamed: 0:0~
    adj_factor:0
    pre_close
注意：
    ricequant中的时间，表示的是k线的结束时间，相当于strendtime
    10分钟的数据14：55最后5分钟没有生成1根K线，相当于10分钟的数据不可用，需要手动合成
'''
import pandas as pd
import time
from datetime import datetime

#时间格式转换
def getEndutc(ricedf):
    print ricedf['index']
    return int(time.mktime(time.strptime(ricedf['index'], '%Y/%m/%d %H:%M')))
def getstrtime(ricedf):
    return datetime.fromtimestamp(ricedf['utc_time'], tz=None).strftime('%Y-%m-%d %H:%M:%S')+'+08:00'
def getstrendtime(ricedf):
    return datetime.fromtimestamp(ricedf['utc_endtime'], tz=None).strftime('%Y-%m-%d %H:%M:%S')+'+08:00'

def transfer(yearlist,minutelist,exchange,sec_id,filehead,datapath):
    for year in yearlist:
        for minute in minutelist:
            bar_type = 60*minute
            print year,bar_type
            filename=filehead+"_%s_%dm.csv"%(year,minute)
            ricedf=pd.read_csv(datapath+filename)
            ricedf['utc_endtime']=ricedf.apply(lambda t: getEndutc(t),axis=1)
            ricedf['utc_time']=ricedf['utc_endtime']-bar_type
            ricedf['strtime']=ricedf.apply(lambda t: getstrtime(t),axis=1)
            ricedf['strendtime']=ricedf.apply(lambda t: getstrendtime(t),axis=1)
            myquantdf=pd.DataFrame()
            myquantdf['strtime']=ricedf['strtime']
            myquantdf['strendtime']=ricedf['strendtime']
            myquantdf['utc_time']=ricedf['utc_time']
            myquantdf['utc_endtime']=ricedf['utc_endtime']
            myquantdf['open']=ricedf['open']
            myquantdf['high']=ricedf['high']
            myquantdf['low']=ricedf['low']
            myquantdf['close']=ricedf['close']
            myquantdf['volume']=ricedf['volume']
            myquantdf['amount']=ricedf['total_turnover']
            myquantdf['position']=ricedf['open_interest']

            myquantdf['bar_type']=bar_type
            myquantdf['exchange']=exchange
            myquantdf['sec_id']=sec_id
            myquantdf['Unnamed: 0']=range(0,ricedf.shape[0])
            myquantdf['adj_factor']=0
            myquantdf['pre_close']=0
            tofilename = "%s.%s %d %s.csv" % (exchange,sec_id,bar_type, year)
            myquantdf.to_csv(datapath+'toMyquant\\'+tofilename)

def rice1mTo10m(yearlist,minutelist,exchange,sec_id,datapath):
    '''
    将ricequant转换成myquant后的1m数据，转换成10m的数据
    主要是解决14：55分的K线问题
    前提：在原始数据中增加range列，表示该K线覆盖的范围
    :return:
    '''

    minute=minutelist[0]
    bar_type=60*minute
    for year in yearlist:
        fname="%s.%s %d %s_for_10m.csv" % (exchange,sec_id,bar_type, year)
        df1m=pd.read_csv(datapath+fname)
        list10m=[]
        i=0
        Unnamed=0
        datalen=df1m.shape[0]
        while i <datalen:
            print i
            starti=i
            endi=i+df1m.ix[i]['range']
            strtime=df1m.ix[starti]['strtime']
            strendtime=df1m.ix[endi-1]['strendtime']
            utc_time=df1m.ix[starti]['utc_time']
            utc_endtime=df1m.ix[endi-1]['utc_endtime']
            open=df1m.ix[starti]['open']
            high= df1m.iloc[starti:endi].high.max()
            low= df1m.iloc[starti:endi].low.min()
            close= df1m.ix[endi-1]['close']
            volume = df1m.iloc[starti:endi].volume.sum()
            amount = df1m.iloc[starti:endi].amount.sum()
            position = df1m.ix[endi-1]['position']
            list10m.append([strtime,strendtime,utc_time,utc_endtime,open,high,low,close,volume,amount,position,600,exchange,sec_id,Unnamed,0,0])
            Unnamed+=1
            i=endi
        df10m=pd.DataFrame(list10m,columns=['strtime','strendtime','utc_time','utc_endtime','open','high','low','close','volume','amount','position','bar_type','exchange','sec_id','Unnamed: 0','adj_factor','pre_close'])
        tofname = "%s.%s %d %s.csv" % (exchange, sec_id, 600, year)
        df10m.to_csv(datapath+tofname)
    pass

def rice1mTo1h(yearlist,minutelist,exchange,sec_id,datapath):
    '''
    将ricequant转换成myquant后的1m数据，转换成1h的数据
    前提：
    复用10m的转换数据，在原始数据中增加range列，表示该K线10m的覆盖范围
    :return:
    '''
    minute=minutelist[0]
    bar_type=60*minute
    list1h = []
    for year in yearlist:
        fname="%s.%s %d %s_for_10m.csv" % (exchange,sec_id,bar_type, year)
        df1m=pd.read_csv(datapath+fname)
        i=0
        Unnamed=0
        datalen=df1m.shape[0]
        range=0
        starti=0
        endi=0
        while i <datalen:
            print i
            r=df1m.ix[i]['range']
            i+=r
            range+=r
            if range==60 or r==5:#满1小时或者到达下午收盘点
                range=0
                endi=i
                strtime=df1m.ix[starti]['strtime']
                strendtime=df1m.ix[endi-1]['strendtime']
                utc_time=df1m.ix[starti]['utc_time']
                utc_endtime=df1m.ix[endi-1]['utc_endtime']
                open=df1m.ix[starti]['open']
                high= df1m.iloc[starti:endi].high.max()
                low= df1m.iloc[starti:endi].low.min()
                close= df1m.ix[endi-1]['close']
                volume = df1m.iloc[starti:endi].volume.sum()
                amount = df1m.iloc[starti:endi].amount.sum()
                position = df1m.ix[endi-1]['position']
                list1h.append([strtime,strendtime,utc_time,utc_endtime,open,high,low,close,volume,amount,position,3600,exchange,sec_id,Unnamed,0,0])
                Unnamed+=1
                starti = i
    df1h=pd.DataFrame(list1h,columns=['strtime','strendtime','utc_time','utc_endtime','open','high','low','close','volume','amount','position','bar_type','exchange','sec_id','Unnamed: 0','adj_factor','pre_close'])
    tofname = "%s.%s %d.csv" % (exchange, sec_id, 3600,)
    df1h.to_csv(datapath+tofname)
    pass

def dataconcat(yearlist,minutelist,exchange,sec_id,datapath):
    '''
    将数据进行拼接
    :return:
    '''
    for minute in minutelist:
        print minute
        df = pd.DataFrame(columns=['strtime', 'strendtime', 'utc_time', 'utc_endtime', 'open', 'high', 'low',
                                      'close', 'volume', 'amount', 'position', 'bar_type', 'exchange', 'sec_id',
                                      'Unnamed: 0', 'adj_factor', 'pre_close'])
        bar_type=minute*60
        for year in yearlist:
            print year
            fname = "%s.%s %d %s.csv" % (exchange, sec_id, bar_type, year)
            df2 = pd.read_csv(datapath+'toMyquant\\'+fname)
            df=pd.concat([df,df2])
        df.drop(labels='Unnamed: 0.1',axis=1,inplace=True)
        df['Unnamed: 0']=range(0,df.shape[0])
        df=df.reset_index(drop=True)
        #df.drop('Unnamed: 0.1')
        tofile="%s.%s %d_rice.csv" % (exchange, sec_id, bar_type)
        df.to_csv(datapath+'toMyquant\\'+tofile)

if __name__ == '__main__':
    yearlist = ['2010','2011','2012','2013', '2014', '2015', '2016','2017']
    exchange = 'SHFE'
    sec_id = 'RB'
    minutelist = [1]
    datapath='D:\\002 MakeLive\DataCollection\\ricequant data\\RB\\'
    filehead='rbdata'
    #transfer(yearlist,minutelist,exchange,sec_id,filehead,datapath)
    rice1mTo1h(yearlist,minutelist,exchange,sec_id,datapath)
    #minutelist=[10]
    #dataconcat(yearlist,minutelist,exchange,sec_id,datapath)
    pass