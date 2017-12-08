# -*- coding: utf-8 -*-
'''
获取所有品种的主力合约代码
制成品种的主力合约切换时刻表
1、获取品种的主力合约，保存为symbol+virtualContract.csv
2、制作主力合约切换表，保存为symbol+ContractSwap.csv
csv格式：
    Symbol oldcontract olddate newcontract newdate
'''
#获取主力合约代码
from gmsdk import *
import pandas as pd
import DATA_CONSTANTS as DC

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
            swaplist.append([symbol.vsymbol,lastsymbol.symbol,lastsymbol.trade_date,symbol.symbol,symbol.trade_date])
        lastsymbol=symbol
    contractdf=pd.DataFrame(s,columns=['vsymbol','symbol','trade_date'])
    contractdf.to_csv('vitualContract\\'+contract+'VirtualContract.csv')

    swapdf=pd.DataFrame(swaplist,columns=['Symbol','oldContract','oldDate','newContract','newDate'])
    swapdf.to_csv('vitualContract\\'+contract+'ContractSwap.csv')