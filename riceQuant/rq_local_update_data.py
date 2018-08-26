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


def update_symbol_data(domain_symbol, contract_map, endutc):
    contract_df = contract_map.loc[contract_map['domain_symbol'] == domain_symbol]
    folder = 'rqdata_raw_%s' % domain_symbol
    for n, rows in contract_df.iterrows():
        if endutc < rows['maturity_utc']:
            # 如果合约的生命周期结束时间在endutc之后，则要更新该合约的数据
            symbol = rows['symbol']
            startdate = rows['listed_date']
            enddate = rows['maturity_date']
            print "updating %s of %s" % (symbol, domain_symbol)
            bardata = get_price(symbol, frequency='1m', start_date=startdate, end_date=enddate)
            bardata.to_csv(folder+"\\rqdata_raw_%s_of_%s_1m.csv" % (symbol, domain_symbol))
    pass


if __name__=="__main__":
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    domain_map = pd.read_excel(DC.PUBLIC_DATA_PATH + 'domainMap.xlsx')
    contract_map = pd.read_csv(DC.PUBLIC_DATA_PATH + 'contractMap.csv')
    enddate = '2018-07-27'
    domain_list = domain_map['symbol'].tolist()
    endutc = int(time.mktime(time.strptime(enddate + ' 16:00:00', '%Y-%m-%d %H:%M:%S')))
    for domain in domain_list:
        update_symbol_data(domain, contract_map, endutc)
    pass
