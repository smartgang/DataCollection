# -*- coding: utf-8 -*-
import DATA_CONSTANTS as DC
import pandas as pd
import numpy
from gmsdk import *

#K_MIN=900
contractlist=pd.read_excel(DC.PUBLIC_DATA_PATH+'Contract.xlsx')['Contract']
tradedatelist={
    'CFFEX':pd.read_csv(DC.PUBLIC_DATA_PATH+'CFFEX tradedates.csv'),
    'CZCE':pd.read_csv(DC.PUBLIC_DATA_PATH+'CZCE tradedates.csv'),
    'DCE':pd.read_csv(DC.PUBLIC_DATA_PATH+'DCE tradedates.csv'),
    'SHFE':pd.read_csv(DC.PUBLIC_DATA_PATH+'SHFE tradedates.csv')
}
K_MIN_set=[60,300,600,900]

def updateRawdata():
    for K_MIN in K_MIN_set:
        for c in contractlist:
            exchange_id,sec_id=c.split('.',1)
            symbol=exchange_id+'.'+sec_id
            datapath = DC.RAW_DATA_PATH + symbol + '\\'
            rawdf=pd.read_csv(datapath+symbol+' '+str(K_MIN)+'.csv')
            md.init(username="smartgang@126.com", password="39314656a")
            tradedates=md.get_calendar(exchange_id, "2017-11-09", "2017-12-05")
            for td in tradedates:
                oprdate=td.strtime[0:10]              ## 交易日
                print symbol + ' ' + str(K_MIN) + ' ' + oprdate
                df1=pd.read_csv(datapath+symbol+oprdate+' '+str(K_MIN)+'.csv')
                rawdf=pd.concat([rawdf,df1])
            rawdf=rawdf.reset_index(drop=True)
            rownum=rawdf.shape[0]
            rawdf['Unnamed: 0']=numpy.arange(0,rownum)
            rawdf.drop('Unnamed: 0.1',axis=1,inplace=True)
            todatapath=datapath = DC.BAR_DATA_PATH + symbol + '\\'
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

updateRawdata()
#concatRawdata()
#concatTicksdata()