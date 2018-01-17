# -*- coding: utf-8 -*-
'''
获取所有品种的主力合约代码
制成品种的主力合约切换时刻表
1、获取品种的主力合约，保存为symbol+virtualContract.csv
2、制作主力合约切换表，保存为symbol+ContractSwap.csv
csv格式：
    Symbol oldcontract olddate newcontract newdate swaputc
'''
#获取主力合约代码
from gmsdk import *
import pandas as pd
import DATA_CONSTANTS as DC
import time

def init():
    contractlist=pd.read_excel(DC.PUBLIC_DATA_PATH+'Contract.xlsx')['Contract']
    md.init(username="smartgang@126.com", password="39314656a")
    for contract in contractlist:
        vsymbols=md.get_virtual_contract(contract, "2016-01-01","2017-12-05")
        swaplist = []
        s=[]
        lastsymbol=vsymbols[0]
        for symbol in vsymbols:
            s.append([
                    symbol.vsymbol,  ##主力合约或连接合约代码
                    symbol.symbol , ##真实symbol
                    symbol.trade_date,  ##交易日
            ])
            if symbol.symbol != lastsymbol.symbol:
                dt = time.strptime(symbol.trade_date + ' 06:00:00', '%Y-%m-%d %H:%M:%S')
                swaputc=int(time.mktime(dt))
                swaplist.append([symbol.vsymbol,lastsymbol.symbol,lastsymbol.trade_date,symbol.symbol,symbol.trade_date,swaputc])
            lastsymbol=symbol
        contractdf=pd.DataFrame(s,columns=['vsymbol','symbol','trade_date'])
        contractdf.to_csv('vitualContract\\'+contract+'VirtualContract.csv')

        swapdf=pd.DataFrame(swaplist,columns=['Symbol','oldContract','oldDate','newContract','newDate','swaputc'])
        swapdf.to_csv('vitualContract\\'+contract+'ContractSwap.csv')

def findSwap(filedf):
    #Symbol	oldContract	oldDate	newContract	newDate	swaputc
    df=pd.DataFrame()
    df['Symbol']=filedf.vsymbol
    df['oldContract']=filedf.symbol
    df['oldDate']=filedf.trade_date
    df['newContract']=filedf.symbol.shift(-1)
    df['newDate'] = filedf.trade_date.shift(-1)
    swaploc=df.loc[df['oldContract']!=df['newContract']]
    swaploc.reset_index(inplace=True,drop=True)
    swaploc.drop([swaploc.shape[0]-1],inplace=True)

    def getutc(ricedf):
        return int(time.mktime(time.strptime(ricedf['newDate']+' 06:00:00', '%Y/%m/%d %H:%M:%S')))

    swaploc['swaputc'] = swaploc.apply(lambda t: getutc(t), axis=1)

    swaploc.to_csv('D:\\002 MakeLive\DataCollection\\vitualContract\SHFE.RBContractSwap.csv')

if __name__ == '__main__':
    filedf=pd.read_csv('D:\\002 MakeLive\DataCollection\\vitualContract\SHFE.RBVirtualContract.csv')
    findSwap(filedf)
    pass