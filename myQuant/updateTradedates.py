# -*- coding: utf-8 -*-
'''
用于更新交易日列表：该列表主要用于ticks数据使用上
格式：
exchange_id,utc_time,strtime
四个交易所在同一张表上
'''
from gmsdk import *
import pandas as pd
import DATA_CONSTANTS as DC

if __name__ =='__main__':
    #参数配置
    startdate='2010-01-01'
    enddate='2018-04-01'

    exchangeList=['SHFE','DCE','CFFEX','CZCE']
    md.init(username="smartgang@126.com", password="39314656a")
    tradedatesList = []
    print ("Updating tradedates from %s to %s" %(startdate,enddate))
    for exchange in exchangeList:
        tradedates = md.get_calendar(exchange, startdate,enddate)
        for t in tradedates:
            l = [
                exchange,
                t.utc_time,
                t.strtime[0:10]
            ]
            tradedatesList.append(l)
    df = pd.DataFrame(tradedatesList,
                          columns=['exchange_id', 'utc_time','strtime'])
    df.to_csv(DC.PUBLIC_DATA_PATH + 'TradeDates.csv')
    print "tradedate updated"