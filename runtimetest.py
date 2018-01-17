# -*- coding: utf-8 -*-
from gmsdk import *
import pandas as pd
import numpy as np
from datetime import datetime
import time
import os
import DATA_CONSTANTS as DC

'''
md.init(username="smartgang@126.com", password="39314656a")
df=pd.read_excel('ContractList2.xlsx')
contractlist=df['Contract']
for c in contractlist:
    exchange_id,sec_id=c.split('.',1)
    symbol=exchange_id+'.'+sec_id
    bars=md.get_dailybars(symbol, '2017-08-15','2017-08-16')#获取上一个交易日最后的持仓量
    print c
    if bars:bars[0].position
    else :print c+' no data'
'''
'''
#创建品种文件夹
contractlist=pd.read_excel(DC.PUBLIC_DATA_PATH+'Contract.xlsx')['Contract']
for c in contractlist:
    os.mkdir(c)
'''
'''
md.init(username="smartgang@126.com", password="39314656a")
exchangelist={'CFFEX','CZCE','DCE','SHFE'}
startdate="2016-01-01"
enddate="2017-10-17"
for exchange_id in exchangelist:
    tradedates=md.get_calendar(exchange_id, startdate, enddate)
    oprdate=[]
    for td in tradedates:
        oprdate.append(td.strtime[0:10])              ## 交易日
    df=pd.DataFrame({'tradedate':oprdate})
    df.to_csv('public data\\'+exchange_id+' tradedates.csv')
'''
'''
#读取中文路径
path='D:\\002 买菜\掘金量化\PositionWin\\backtestMA.csv'
upath=unicode(path,'utf-8')
df=pd.read_csv(upath)
print df.head(3)
'''
'''
tradedatelist={
    'CFFEX':(pd.read_csv(DC.PUBLIC_DATA_PATH+'CFFEX tradedates.csv')),
    'CZCE':(pd.read_csv(DC.PUBLIC_DATA_PATH+'CZCE tradedates.csv')),
    'DCE':(pd.read_csv(DC.PUBLIC_DATA_PATH+'DCE tradedates.csv')),
    'SHFE':(pd.read_csv(DC.PUBLIC_DATA_PATH+'SHFE tradedates.csv'))
}

tradedate=tradedatelist['CFFEX']
tradedate.index=pd.to_datetime(tradedate['tradedate'])
print tradedate.tail(3)
print tradedate.truncate(before='2017-10-11')
'''

#将ContractSwap中的date定位到源文件中的index
contractlist=pd.read_excel(DC.PUBLIC_DATA_PATH+'ContractList.xlsx')['Contract']
for symbol in contractlist:
    swapdf=pd.read_csv('D:\\002 MakeLive\DataCollection\\vitualContract\\'+symbol+'ContractSwap.csv')
    newdate=swapdf['newDate']
    timeStamplist=[]
    for nd in newdate:
        dt=time.strptime(nd+' 09:00:00','%Y/%m/%d %H:%M:%S')
        timeStamplist.append(int(time.mktime(dt)))
    K_MIN_set=[60,300,600,900]
    for K_MIN in K_MIN_set:
        print ('symbols:%s,K_MIN:%d' %(symbol,K_MIN))
        rawdf=pd.read_csv('D:\\002 MakeLive\DataCollection\\bar data\\'+symbol+'\\'+symbol+' '+str(K_MIN)+'.csv')
        indexlist=[]
        for i in timeStamplist:
            print i
            l=rawdf.loc[rawdf['utc_time']==i]
            while l.shape[0]==0:
                i+=60
                l = rawdf.loc[rawdf['utc_time'] == i]
            indexlist.append(l.index[0])
        swapdf[str(K_MIN)]=indexlist
    swapdf.to_csv('D:\\002 MakeLive\DataCollection\\vitualContract\\'+symbol+'ContractSwap.csv')
'''
s=[]
lastsymbol=vsymbols[0]
for symbol in vsymbols:
    s.append([
            symbol.vsymbol,  ##主力合约或连接合约代码
            symbol.symbol , ##真实symbol
            symbol.trade_date,  ##交易日
    ])
    if symbol.symbol != lastsymbol.symbol:
        swaplist.append([symbol.vsymbol,lastsymbol.symbol,lastsymbol.trade_date,symbol.symbol,symbol.trade_date])
    lastsymbol=symbol
contractdf=pd.DataFrame(s,columns=['vsymbol','symbol','trade_date'])
contractdf.to_csv('vitualContract\\'+contract+'VirtualContract.csv')

swapdf=pd.DataFrame(swaplist,columns=['Symbol','oldContract','oldDate','newContract','newDate'])
swapdf.to_csv('vitualContract\\'+contract+'ContractSwap.csv')
'''