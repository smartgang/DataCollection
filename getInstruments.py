# -*- coding: utf-8 -*-
'''
#获取交易代码数据
#class Instrument(object):
#    def __init__(self):
#        self.symbol = ''                ## 交易代码
#        self.sec_type = 0               ## 代码类型
#        self.sec_name = ''              ## 代码名称
#        self.multiplier = 0.0           ## 合约乘数
#        self.margin_ratio = 0.0         ## 保证金比率
#        self.price_tick = 0.0           ## 价格最小变动单位
#        self.upper_limit = 0.0          ## 当天涨停板
#        self.lower_limit = 0.0          ## 当天跌停板
#        self.is_active = 0              ## 当天是否交易
#        self.update_time = ''           ## 更新时间
'''
from gmsdk import *
import pandas as pd

md.init(username="smartgang@126.com", password="39314656a")
exchangeidlist=['SHFE','DCE','CFFEX','CZCE']
for exchange in exchangeidlist:
    instruments=md.get_instruments(exchange, 4, 1)
    instrumentslist=[]
    for ins in instruments:
        l=[
        ins.symbol,
        ins.sec_type,             ## 代码类型
        ins.sec_name ,              ## 代码名称
        ins.multiplier,         ## 合约乘数
        ins.margin_ratio,        ## 保证金比率
        ins.price_tick ,           ## 价格最小变动单位
        ins.upper_limit,          ## 当天涨停板
        ins.lower_limit,          ## 当天跌停板
        ins.is_active,              ## 当天是否交易
        ins.update_time           ## 更新时间
        ]
        instrumentslist.append(l)
    df=pd.DataFrame(instrumentslist,columns=['symbol','sec_type','sec_name','multiplier','margin_ratio','price_tick','upper_limit','lower_limit','is_active','update_time'])
    df.to_csv(exchange+' Instruments.csv')