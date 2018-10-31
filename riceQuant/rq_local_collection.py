# -*- coding: utf-8 -*-
import rqdatac as rq
from rqdatac import *
import pandas as pd
import DATA_CONSTANTS as DC
import os
from datetime import datetime
rq.init()


def colletion_test():
    startdate = '2018-09-01'
    enddate = '2018-09-05'
    id = 'J1901'
    freq = '1m'
    #freq = 'tick'
    bardata = get_price(id, frequency=freq, start_date=startdate, end_date=enddate)
    bardata.to_csv("%s_%s_1m.csv" % (id, enddate))
    print ("%s %s from %s to %s collection finished" % (id, freq, startdate, enddate))


def collection_by_symbol():
    domain_symbol = 'CFFEX.IF'
    symbol_list = ['IF1807', 'IF1808', 'IF1809']
    print ("start collect:%s" % domain_symbol)
    symbolinfo = DC.SymbolInfo(domain_symbol)
    folder = "rqdata_raw_%s" % domain_symbol
    try:
        os.mkdir(folder)
    except:
        pass
    for symbol in symbol_list:
        startdate, enddate = symbolinfo.getSymbolLifeDate(symbol)
        bardata = get_price(symbol, frequency='1m', start_date=startdate, end_date=enddate)
        bardata.to_csv(folder+"\\rqdata_raw_%s_of_%s_1m.csv" % (symbol, domain_symbol))
        print ("%s of %s from %s to %s collection finished" % (symbol, domain_symbol, startdate, enddate))
    print ("finish collect:%s" % domain_symbol)
    print ("all work done!")


def collection_domain_by_symbol():
    # 抓取全品种全合约数据
    domain_list = pd.read_excel(DC.PUBLIC_DATA_PATH+'domainMap.xlsx')['symbol']
    for domain_symbol in domain_list:
        print ("start collect:%s" % domain_symbol)
        symbolinfo = DC.SymbolInfo(domain_symbol)
        folder = "rqdata_raw_%s" % domain_symbol
        try:
            os.mkdir(folder)
        except:
            pass
        for symbol in symbolinfo.getSymbolList():
            startdate, enddate = symbolinfo.getSymbolLifeDate(symbol)
            bardata = get_price(symbol, frequency='1d', start_date=startdate, end_date=enddate)
            bardata.to_csv(folder+"\\rqdata_raw_%s_of_%s_1d.csv" % (symbol, domain_symbol))
            print ("%s of %s from %s to %s collection finished" % (symbol, domain_symbol, startdate, enddate))
        print ("finish collect:%s" % domain_symbol)
    print ("all work done!")


def collection_tick_by_symbol(domain, symbol):
    # 抓某特定品种合约主力期间的tick数据，按天保存
    folder = "rqdata_raw_%s" % domain +'\\raw_tick_%s' % symbol
    try:
        os.mkdir(folder)
    except:
        pass
    contractMapDf = pd.read_csv(DC.PUBLIC_DATA_PATH + 'contractMap.csv', index_col='symbol')
    #symbol_domain_start = contractMapDf.ix[symbol, 'domain_start_date'].replace('\\', '-')
    #symbol_domain_end = contractMapDf.ix[symbol, 'domain_end_date'].replace('\\', '-')
    symbol_domain_start = '2018-03-21'
    symbol_domain_end = '2018-03-28'
    datelist = [datetime.strftime(x, '%Y-%m-%d') for x in list(pd.date_range(start=symbol_domain_start, end=symbol_domain_end, freq='D', normalize=True, closed='right'))]
    datelist.insert(0, symbol_domain_start)
    for d in datelist:
        print ("start collect:%s %s" % (symbol,d))
        tickdata = get_price(symbol, frequency='tick', start_date=d, end_date=d)
        tickdata.to_csv(folder + "\\raw_tick_%s_%s.csv" % (symbol, d))
    print ("all work done!")


def collection_domain():
    # 抓取全品种主连1m数据
    domainMap = pd.read_excel(DC.PUBLIC_DATA_PATH+'domainMap.xlsx')
    startdate = '2010-01-01'
    enddate = '2018-07-20'
    for index, row in domainMap.iterrows():
        book_id = row[0]
        domain_symbol = row[2]
        print ("start collect:%s %s" % (domain_symbol, book_id))
        folder = "rqdata_raw_%s" % domain_symbol
        bardata = get_price(book_id, frequency='1m', start_date=startdate, end_date=enddate)
        bardata.to_csv(folder + "\\rqdata_raw_%s_of_%s_1m.csv" % (domain_symbol, domain_symbol))
        print ("%s of %s from %s to %s collection finished" % (domain_symbol, domain_symbol, startdate, enddate))
    print ("all work done!")


if __name__=='__main__':
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    colletion_test()
    #collection_by_symbol()  # 抓某个品种的特定多个合约
    #collection_domain_by_symbol()
    #collection_tick_by_symbol('SHFE.RB', 'RB1805')
    #collection_domain()
