# -*- coding: utf-8 -*-
"""
根据contractMap记录的合约下市时间，更新合约数据
如果合约的下市时间在enddate之后，说明在上次更新后到enddate还有新的数据，要一并更新
"""
import DATA_CONSTANTS as DC
import pandas as pd
import time
import os
import rqdatac as rq
from rqdatac import *
rq.init()


def update_symbol_data(domain_symbol, contract_map, last_update_utc):
    contract_df = contract_map.loc[contract_map['domain_symbol'] == domain_symbol]
    folder = 'rqdata_raw_%s' % domain_symbol
    for n, rows in contract_df.iterrows():
        #if 1532678400 < rows['maturity_utc'] < 1535702400:
        if last_update_utc < rows['maturity_utc']:
            # 如果合约的生命周期结束时间在endutc之后，则要更新该合约的数据
            symbol = rows['symbol']
            startdate = rows['listed_date']
            enddate = rows['maturity_date']
            print "updating %s of %s" % (symbol, domain_symbol)
            bardata = get_price(symbol, frequency='1m', start_date=startdate, end_date=enddate)
            bardata.to_csv(folder+"\\rqdata_raw_%s_of_%s_1m.csv" % (symbol, domain_symbol))
    pass


def update_symbol_domain_data(domain_map, enddate):
    for n, rows in domain_map.iterrows():
        order_book_id = rows['order_book_id']
        domain_symbol = rows['symbol']
        folder = 'rqdata_raw_%s' % domain_symbol
        print "updating domain data of %s" % (domain_symbol)
        bardata = get_price(order_book_id, frequency='1m', start_date='20100101', end_date=enddate)
        bardata.to_csv(folder+"\\rqdata_raw_domain_of_%s_1m.csv" % (domain_symbol))
    pass


if __name__=="__main__":
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    domain_map = pd.read_excel(DC.PUBLIC_DATA_PATH + 'domainMap.xlsx')
    contract_map = pd.read_csv(DC.PUBLIC_DATA_PATH + 'contractMap.csv')
    late_update_date = '2018-09-01'
    domain_list = domain_map['symbol'].tolist()
    last_update_utc = int(time.mktime(time.strptime(late_update_date + ' 16:00:00', '%Y-%m-%d %H:%M:%S')))
    print last_update_utc
    for domain in domain_list:
        update_symbol_data(domain, contract_map, last_update_utc)
    #update_symbol_domain_data(domain_map, enddate)
    pass
