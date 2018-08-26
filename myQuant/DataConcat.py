# -*- coding: utf-8 -*-
import DATA_CONSTANTS as DC
import pandas as pd
import numpy
import time
from gmsdk import *

def updateRawdataByDay():
    for K_MIN in K_MIN_set:
        for c in contractlist:
            exchange_id,sec_id=c.split('.',1)
            symbol=exchange_id+'.'+sec_id
            datapath = DC.RAW_DATA_PATH + symbol + '\\'
            rawdf=pd.read_csv(datapath+symbol+' '+str(K_MIN)+'.csv')
            md.init(username="smartgang@126.com", password="39314656a")
            tradedates=md.get_calendar(exchange_id, "2017-12-05", "2017-12-30")
            for td in tradedates:
                oprdate=td.strtime[0:10]              ## 交易日
                print symbol + ' ' + str(K_MIN) + ' ' + oprdate
                df1=pd.read_csv(datapath+symbol+oprdate+' '+str(K_MIN)+'.csv')
                rawdf=pd.concat([rawdf,df1])
            rawdf=rawdf.reset_index(drop=True)
            rownum=rawdf.shape[0]
            #rawdf['Unnamed: 0']=numpy.arange(0,rownum)
            rawdf.drop('Unnamed: 0',axis=1,inplace=True)
            todatapath= DC.BAR_DATA_PATH + symbol + '\\'
            rawdf.to_csv(todatapath+symbol+' '+str(K_MIN)+'.csv')

def updateRawdataByPeriod(K_MIN_set,contractlist,startdate,enddate):
    #将raw data文件夹中某一段时间的数据，拼接到bar data中
    #每月下载数据后，使用此函数更新数据
    for K_MIN in K_MIN_set:
        for c in contractlist:
            exchange_id,sec_id=c.split('.',1)
            symbol=exchange_id+'.'+sec_id
            datapath = DC.RAW_DATA_PATH + symbol + '\\'
            todatapath = DC.BAR_DATA_PATH + symbol + '\\'
            rawdf=pd.read_csv(todatapath+symbol+' '+str(K_MIN)+'.csv')
            startutc = float(time.mktime(time.strptime(startdate+" 00:00:00", "%Y-%m-%d %H:%M:%S")))
            rawdf = rawdf.loc[(rawdf['utc_time'] < startutc) ]
            print symbol + ' ' + str(K_MIN) + ' ' + enddate
            df1=pd.read_csv(datapath+symbol+enddate+' '+str(K_MIN)+'.csv')
            df1=df1.loc[df1['utc_time']> startutc]
            rawdf=pd.concat([rawdf,df1])
            rawdf.reset_index(drop=True,inplace=True)
            rownum=rawdf.shape[0]
            rawdf['Unnamed: 0']=numpy.arange(0,rownum)
            rawdf.drop('Unnamed: 0.1',axis=1,inplace=True)

            rawdf.to_csv(todatapath+symbol+' '+str(K_MIN)+'.csv')

def insertRawdataByPeriod(K_MIN_set,contractlist,startdate,enddate):
    #将raw data文件夹中某一段时间的数据，拼接到bar data中
    #每月下载数据后，使用此函数更新数据
    for K_MIN in K_MIN_set:
        for c in contractlist:
            exchange_id,sec_id=c.split('.',1)
            symbol=exchange_id+'.'+sec_id
            datapath = DC.RAW_DATA_PATH + symbol + '\\'
            todatapath = DC.BAR_DATA_PATH + symbol + '\\'
            rawdf=pd.read_csv(todatapath+symbol+' '+str(K_MIN)+'.csv')
            startutc = float(time.mktime(time.strptime(startdate + " 00:00:00", "%Y-%m-%d %H:%M:%S")))
            endutc = float(time.mktime(time.strptime(enddate + " 23:59:59", "%Y-%m-%d %H:%M:%S")))
            headdf = rawdf.loc[(rawdf['utc_time'] < startutc)]
            taildf = rawdf.loc[(rawdf['utc_time'] >= endutc)]
            df1 = pd.read_csv(datapath+symbol+enddate+' '+str(K_MIN)+'.csv')
            df1 = df1.loc[(df1['utc_time'] >= startutc) & (df1['utc_time'] < endutc)]
            rawdf = pd.concat([headdf, df1, taildf])
            rawdf.reset_index(drop=True,inplace=True)
            rownum=rawdf.shape[0]
            rawdf['Unnamed: 0']=numpy.arange(0,rownum)
            rawdf.drop('Unnamed: 0.1',axis=1,inplace=True)
            print ("insert data of %s %d from %s to %s finished!" % (symbol, K_MIN, startdate, enddate))
            rawdf.to_csv(todatapath+symbol+' '+str(K_MIN)+'.csv')

def concatRawdata():
    for symbol in contractlist:
        df = pd.DataFrame()
        exchange_id, sec_id = symbol.split('.', 1)
        datapath = DC.RAW_DATA_PATH + symbol + '\\'
        print symbol
        md.init(username="smartgang@126.com", password="39314656a")
        tradedates=md.get_calendar(exchange_id, "2016-01-01", "2017-11-09")
        for td in tradedates:
            oprdate = td.strtime[0:10]
            print oprdate
            df1=pd.read_csv(datapath+symbol+oprdate+' '+str(K_MIN)+'.csv')
            df=pd.concat([df,df1])
        df=df.reset_index(drop=True)
        rownum=df.shape[0]
        df['Unnamed: 0']=numpy.arange(0,rownum)
        df.to_csv(datapath+symbol+' '+str(K_MIN)+'.csv')
        del df

def concatTicksdata():
    for symbol in contractlist:
        df = pd.DataFrame()
        print symbol
        exchange_id,sec_id=symbol.split('.',1)

        tradedates = tradedatelist[exchange_id]
        tradedates.index = pd.to_datetime(tradedates['tradedate'])
        tradedate = tradedates.truncate(before=DC.TICKS_DATA_START_DATE)['tradedate']

        datapath= DC.TICKS_DATA_PATH + symbol + '\\' + symbol
        for oprdate in tradedate:
            df1=pd.read_csv(datapath+oprdate+'ticks ' + str(K_MIN)+'.csv')
            df=pd.concat([df,df1])
        df = df.reset_index(drop=True)
        rownum = df.shape[0]
        df['Unnamed: 0'] = numpy.arange(0, rownum)
        df.to_csv(datapath+'ticks ' + str(K_MIN)+'.csv')

def copyRawtoBar(K_MIN_set,contractlist,enddate):
    for K_MIN in K_MIN_set:
        for c in contractlist:
            exchange_id,sec_id=c.split('.',1)
            symbol=exchange_id+'.'+sec_id
            datapath = DC.RAW_DATA_PATH + symbol + '\\'
            todatapath = DC.BAR_DATA_PATH + symbol + '\\'
            rawdf=pd.read_csv(datapath+symbol+enddate+' '+str(K_MIN)+'.csv')
            print symbol + ' ' + str(K_MIN) + ' ' + enddate
            rawdf.to_csv(todatapath+symbol+' '+str(K_MIN)+'.csv')



if __name__ == '__main__':
    contractlist=pd.read_excel(DC.PUBLIC_DATA_PATH+'Contract.xlsx')['Contract']
    tradedatelist={
        'CFFEX':pd.read_csv(DC.PUBLIC_DATA_PATH+'CFFEX tradedates.csv'),
        'CZCE':pd.read_csv(DC.PUBLIC_DATA_PATH+'CZCE tradedates.csv'),
        'DCE':pd.read_csv(DC.PUBLIC_DATA_PATH+'DCE tradedates.csv'),
        'SHFE':pd.read_csv(DC.PUBLIC_DATA_PATH+'SHFE tradedates.csv')
    }
    K_MIN_set=[0]
    startdate="2018-02-01"
    enddate="2018-03-01"
    #updateRawdataByPeriod(K_MIN_set,contractlist,startdate,enddate)
    #concatRawdata()
    #concatTicksdata()
    copyRawtoBar(K_MIN_set,contractlist,enddate)