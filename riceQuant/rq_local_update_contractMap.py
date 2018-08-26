# -*- coding: utf-8 -*-

import rqdatac as rq
from rqdatac import *
import pandas as pd
import DATA_CONSTANTS as DC
import os
from datetime import datetime
import time

rq.init()

startdate = '2014-01-01'
enddate = '2015-04-16'

bookid = pd.read_excel(DC.PUBLIC_DATA_PATH + 'domainMap.xlsx')['book_id'].tolist()
domain_map = pd.DataFrame()
for id in bookid:
    domain_map[id] = rq.get_dominant_future(id, start_date=startdate, end_date=enddate)
    print ("%s finished" % id)
domain_map.to_csv('domain_list %s to %s.csv' % (startdate, enddate))

symboldf = pd.read_csv(DC.PUBLIC_DATA_PATH + 'contractMap.csv', index_col='symbol')
exist_symbol_list = symboldf.index.tolist()
domain_map = pd.read_csv('domain_list %s to %s.csv' % (startdate, enddate))
rice_book_list = symboldf['rice_book_id'].drop_duplicates().tolist()
for id in rice_book_list:
    grouped = domain_map.groupby(id)
    first_date_list = grouped['date'].first()
    last_date_list = grouped['date'].last()
    symbollist = first_date_list.index.tolist()
    for symbol in symbollist:
        last_date = last_date_list[symbol]
        if symbol in exist_symbol_list:  # 如果该合约已存在，则更新last_date
            print ('update contract %s' % symbol)
            symboldf.ix[symbol, 'domain_end_date'] = last_date
            symboldf.ix[symbol, 'domain_end_utc'] = int(time.mktime(time.strptime(last_date + ' 16:00:00', '%Y-%m-%d %H:%M:%S')))
        else:  # 如果该合约不存在，则为新合约，则要添加该条纪录
            print ('new contract %s' % symbol)
            ins = instruments(symbol, country='cn')
            exchange = ins.exchange
            sec = ins.underlying_symbol
            domain_symbol = exchange + '.' + sec
            first_date = first_date_list[symbol]
            symboldf.ix[symbol, 'domain_start_date'] = first_date
            symboldf.ix[symbol, 'domain_end_date'] = last_date
            symboldf.ix[symbol, 'domain_symbol'] = domain_symbol
            symboldf.ix[symbol, 'rice_domain_id'] = sec + '88'
            symboldf.ix[symbol, 'rice_book_id'] = sec
            symboldf.ix[symbol, 'exchange'] = exchange
            symboldf.ix[symbol, 'sec'] = sec
            predate = get_previous_trading_date(first_date, country='cn')
            symboldf.ix[symbol, 'domain_start_utc'] = int(time.mktime(time.strptime(predate.strftime("%Y-%m-%d") + ' 21:00:00', '%Y-%m-%d %H:%M:%S')))
            symboldf.ix[symbol, 'domain_end_utc'] = int(time.mktime(time.strptime(last_date + ' 16:00:00', '%Y-%m-%d %H:%M:%S')))
            symboldf.ix[symbol, 'margin_rate'] = ins.margin_rate
            symboldf.ix[symbol, 'abbrev_symbol'] = ins.abbrev_symbol
            symboldf.ix[symbol, 'listed_date'] = ins.listed_date
            symboldf.ix[symbol, 'listed_utc'] = int(time.mktime(time.strptime(ins.listed_date + ' 21:00:00', '%Y-%m-%d %H:%M:%S')))
            symboldf.ix[symbol, 'type'] = ins.type
            symboldf.ix[symbol, 'contract_multiplier'] = ins.contract_multiplier
            symboldf.ix[symbol, 'maturity_date'] = ins.maturity_date
            symboldf.ix[symbol, 'maturity_utc'] = int(time.mktime(time.strptime(ins.maturity_date + ' 16:00:00', '%Y-%m-%d %H:%M:%S')))
            symboldf.ix[symbol, 'settlement_method'] = ins.settlement_method
            symboldf.ix[symbol, 'product'] = ins.product
            symboldf.ix[symbol, 'price_tick'] = ins.tick_size()
symboldf.to_csv('contractMap1.csv')
print ('all contract updated!')
