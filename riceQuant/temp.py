# -*- coding: utf-8 -*-
"""临时处理用"""
import pandas as pd
import time
import rqdatac as rq
from rqdatac import *

def contractMap_add_life_utc():
    # 为contractMap文件增加life_utc列
    cm = pd.read_csv('D:\\002 MakeLive\DataCollection\public data\\contractMap.csv')
    listed_utc_list = []
    maturity_utc_list = []
    for n, rows in cm.iterrows():
        listed_utc = int(time.mktime(time.strptime(rows['listed_date'] + '  21:00:00', '%Y-%m-%d %H:%M:%S')))
        maturity_utc = int(time.mktime(time.strptime(rows['maturity_date'] + ' 16:00:00', '%Y-%m-%d %H:%M:%S')))
        listed_utc_list.append(listed_utc)
        maturity_utc_list.append(maturity_utc)
    cm['listed_utc'] = listed_utc_list
    cm['maturity_utc'] = maturity_utc_list
    cm.to_csv('contractMap2.csv')
    pass


def contractMap_mod_domain_utc():
    rq.init()
    # 为contractMap文件增加life_utc列
    cm = pd.read_csv('D:\\002 MakeLive\DataCollection\public data\\contractMap.csv')
    domain_start_utc_list = []
    domain_end_utc_list = []
    cm['domain_start_date'] = cm['domain_start_date'].str.replace('\\', '-')
    cm['domain_end_date'] = cm['domain_end_date'].str.replace('\\', '-')
    cm['listed_date'] = cm['listed_date'].str.replace('\\', '-')
    cm['maturity_date'] = cm['maturity_date'].str.replace('\\', '-')
    for n, rows in cm.iterrows():
        first_date = rows['domain_start_date']
        predate = get_previous_trading_date(first_date, country='cn')
        domain_start_utc = int(time.mktime(time.strptime(predate.strftime("%Y-%m-%d") + '  21:00:00', '%Y-%m-%d %H:%M:%S')))
        domain_end_utc = int(time.mktime(time.strptime(rows['domain_end_date'] + ' 16:00:00', '%Y-%m-%d %H:%M:%S')))
        domain_start_utc_list.append(domain_start_utc)
        domain_end_utc_list.append(domain_end_utc)
    cm['domain_start_utc'] = domain_start_utc_list
    cm['domain_end_utc'] = domain_end_utc_list
    #cm.drop('Unnamed: 0.1', axis=1, inplace=True)
    #cm['Unnamed: 0']=range(cm.shape[0])
    cm.to_csv('contractMap2.csv', index=False)
    pass

def contract_domain_time_check():
    # 检查哪些品种的主连时间存在重叠的情况
    cm = pd.read_csv('D:\\002 MakeLive\DataCollection\public data\\contractMap.csv')
    cm.sort_values('domain_start_utc', inplace=True)
    domain_symbol_list = cm['domain_symbol'].drop_duplicates().tolist()
    for domain_symbol in domain_symbol_list:
        cm1 = cm.loc[cm['domain_symbol'] == domain_symbol]
        domain_end_utc_list = cm1['domain_end_utc'].tolist()
        domain_start_utc_list = cm1['domain_start_utc'].tolist()
        symbol_list = cm1['symbol'].tolist()
        for i in range(cm1.shape[0]-1):
            domain_end_utc = domain_end_utc_list[i]
            if domain_end_utc > min(domain_start_utc_list[i+1:]):
                print ("%s of %s error" % (symbol_list[i], domain_symbol))


def get_trading_date():
    #rq.init()
    tl = get_trading_dates('20100101', '20180816')
    tl2 = []
    for t in tl:
        tl2.append(t.strftime("%Y-%m-%d"))
    df = pd.DataFrame(tl2, columns=['trading_date'])
    df.to_csv('trading_date_list.csv')


def get_date_utc(df):
    # print ricedf['Unnamed: 0']
    # 有些的时间列没有index 的列名，要使用'Unnamed: 0'
    return int(time.mktime(time.strptime(df['previous_date'] + ' 21:00:00', '%Y-%m-%d %H:%M:%S')))


def process_previous_trading_date():
    cm = pd.read_csv('D:\\002 MakeLive\DataCollection\public data\\trading_date_list.csv', index_col='Unnamed: 0')
    #cm['previous_date'] = cm['trading_date'].shift(1)
    #cm.ix[0, 'previous_date'] = cm.ix[0, 'trading_date']
    #cm.drop('Unnamed: 0', axis=1)
    cm['utc_time'] = cm.apply(lambda t: get_date_utc(t), axis=1)
    cm.ix[0, 'utc_time'] = int(time.mktime(time.strptime(cm.ix[0, 'previous_date'] + ' 09:00:00', '%Y-%m-%d %H:%M:%S')))
    cm.to_csv('D:\\002 MakeLive\DataCollection\public data\\trading_date_list.csv', index=False)

if __name__=="__main__":
    #contractMap_add_life_utc()
    contractMap_mod_domain_utc()
    #contract_domain_time_check()
    #get_trading_date()
    #process_previous_trading_date()
    pass