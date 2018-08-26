# -*- coding: utf-8 -*-
"""
根据单个合约的主力时间，将多个单合约拼接成一个主连数据
"""
import time
import pandas as pd
from datetime import datetime
import os

def symbolsToDomain(bar_type, domain_symbol, symbolMap):
    rawfolder = ('D:\\002 MakeLive\DataCollection\\bar data\\'+"%s\\" % domain_symbol)

    symbolMapdf = symbolMap.loc[symbolMap['domain_symbol'] == domain_symbol]
    symbollist = symbolMapdf['symbol'].tolist()
    symbolMapdf.set_index('symbol', inplace=True, drop=True)
    domaindf = pd.DataFrame()
    # 第一个合约只截后面，不截前面，因为米筐的规则新上市第一天不算主力，照米筐给的主力时间取的话，第一天会丢失，这样会导致后面生成60adj第一天缺失
    symbol0 = symbollist[0]
    print (symbol0)
    domain_end_utc = symbolMapdf.ix[symbol0, 'domain_end_utc']
    fname = "%s %d.csv" % (symbol0, bar_type)
    symbolAllDf = pd.read_csv(rawfolder + fname)
    symbolDomain = symbolAllDf.loc[symbolAllDf['utc_endtime'] < domain_end_utc]
    domaindf = pd.concat([domaindf, symbolDomain])
    for symbol in symbollist[1:-1]:
        print (symbol)
        domain_start_utc = symbolMapdf.ix[symbol, 'domain_start_utc']
        domain_end_utc = symbolMapdf.ix[symbol, 'domain_end_utc']
        fname = "%s %d.csv" % (symbol, bar_type)
        symbolAllDf = pd.read_csv(rawfolder + fname)
        symbolDomain = symbolAllDf.loc[(symbolAllDf['utc_time'] >= domain_start_utc) & (symbolAllDf['utc_endtime'] < domain_end_utc) ]
        domaindf = pd.concat([domaindf, symbolDomain])
    # 最后一个合约只截前面，不截后面，因为后面可能会有下一个交易日夜盘的数据
    symbol_last = symbollist[-1]
    print (symbol_last)
    domain_start_utc = symbolMapdf.ix[symbol_last, 'domain_start_utc']
    fname = "%s %d.csv" % (symbol_last, bar_type)
    symbolAllDf = pd.read_csv(rawfolder + fname)
    symbolDomain = symbolAllDf.loc[symbolAllDf['utc_time'] >= domain_start_utc]
    domaindf = pd.concat([domaindf, symbolDomain])

    domaindf.sort_values('utc_time')
    domaindf.to_csv(rawfolder + "%s %d.csv" % (domain_symbol, bar_type), index=False)



if __name__ == '__main__':
    os.chdir('D:\\002 MakeLive\DataCollection\\bar data')
    month_mode = False   # 为True表示是月度更新模式，只更新该月涉及的合约
    if month_mode:
        enddate = '2018-07-01'
        domain_map = pd.read_excel('D:\\002 MakeLive\DataCollection\public data\\domainMap.xlsx')
        contract_map = pd.read_csv('D:\\002 MakeLive\DataCollection\public data\\contractMap.csv')
        active_domain_list = domain_map.loc[domain_map['active'] == True]['symbol'].tolist()
        for domain_symbol in active_domain_list:
            symbolsToDomain(60, domain_symbol, contract_map)

    else:
        symboldf = pd.read_csv('D:\\002 MakeLive\DataCollection\public data\\contractMap.csv')
        domain = 'CFFEX.IC'
        bar_type = 60
        symbolsToDomain(bar_type, domain, symboldf)
    pass
