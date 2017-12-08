# -*- coding: utf-8 -*-
import datetime
from gmsdk import *
import pandas as pd
import DATA_CONSTANTS as DC

#获取近2个月的交易日期
#每天取一次数据
#计算出持仓量变化

md.init(username="smartgang@126.com", password="39314656a")
contractlist=pd.read_excel(DC.PUBLIC_DATA_PATH+'Contract.xlsx')['Contract']

for c in contractlist:
    exchange_id,sec_id=c.split('.',1)
    symbol=exchange_id+'.'+sec_id
    tradedates=md.get_calendar(exchange_id, "2017-10-16", "2017-12-05")
    bars=md.get_dailybars(symbol, '2017-10-13','2017-10-13')#获取上一个交易日最后的持仓量
    lastposition=bars[0].position
    for td in tradedates:
        oprdate=td.strtime[0:10]              ## 交易日
        print symbol + ' '+ oprdate
        starttime=oprdate+' 00:00:00'
        endtime=oprdate+' 23:59:59'
        tickbuf=[]
        delta_long=0
        delta_short=0
        nextflag=False
        while (nextflag is not True):
            ticks = md.get_ticks(symbol, starttime, endtime)
            for tick in ticks:
                delta_position=tick.cum_position-lastposition
                if tick.trade_type == 1 or tick.trade_type == 2:
                    delta_long = delta_position / 2
                    delta_short = delta_position / 2
                elif tick.trade_type == 3 or tick.trade_type == 6:
                    delta_long = delta_position
                    delta_short = 0
                elif tick.trade_type == 4 or tick.trade_type == 5:
                    delta_short = delta_position
                    delta_long=0

                if tick.asks:
                    ap1=tick.asks[0][0]
                    av1=tick.asks[0][1]
                else:
                    ap1=0
                    av1=0
                if tick.bids:
                    bp1=tick.bids[0][0]
                    bv1=tick.bids[0][1]
                else:
                    bp1=0
                    bv1=0

                tickbuf.append([
                    tick.exchange,   ## 交易所代码
                    tick.sec_id,   ## 证券ID
                    tick.utc_time,  ## 行情时间戳
                    tick.strtime,   ## 可视化时间
                    tick.last_price,   ## 最新价
                    tick.open,   ## 开盘价
                    tick.high,   ## 最高价
                    tick.low,   ## 最低价

                    tick.cum_volume ,  ## 成交总量/最新成交量,累计值
                    tick.cum_amount ,  ## 成交总金额/最新成交额,累计值
                    tick.cum_position,  ## 合约持仓量(期),累计值
                    delta_position,
                    delta_long,
                    delta_short,
                    tick.last_volume,  ## 瞬时成交量(中金所提供)
                    tick.last_amount,  ## 瞬时成交额

                    tick.upper_limit,  ## 涨停价
                    tick.lower_limit,  ## 跌停价
                    tick.settle_price,  ## 今日结算价
                    tick.trade_type,  ## (保留)交易类型,对应多开,多平等类型
                    tick.pre_close,  ## 昨收价

                    bp1,
                    bv1,
                    #tick.bids[0][0],  ## [(price, volume), (price, volume), ...] ## 1-5档买价,量
                    #tick.bids[0][1],
                    #tick.bids[1][0],
                    #tick.bids[1][1],
                    #tick.bids[2][0],
                    #tick.bids[2][1],
                    #tick.bids[3][0],
                    #tick.bids[3][1],
                    #tick.bids[4][0],
                    #tick.bids[4][1],

                    ap1,
                    av1
                    #tick.asks[0][0],  ## [(price, volume), (price, volume), ...] ## 1-5档卖价,量
                    #tick.asks[0][1],
                    #tick.asks[1][0],  ## [(price, volume), (price, volume), ...] ## 1-5档卖价,量
                    #tick.asks[1][1],
                    #tick.asks[2][0],  ## [(price, volume), (price, volume), ...] ## 1-5档卖价,量
                    #tick.asks[2][1],
                    #tick.asks[3][0],  ## [(price, volume), (price, volume), ...] ## 1-5档卖价,量
                    #tick.asks[3][1],
                    #tick.asks[4][0],  ## [(price, volume), (price, volume), ...] ## 1-5档卖价,量
                    #tick.asks[4][1]
                    ])
                lastposition=tick.cum_position
            if len(tickbuf) == 33000:
                starttime = datetime.datetime.fromtimestamp(tickbuf[-1][2]).strftime('%Y-%m-%d %H:%M:%S')
            else:
                nextflag = True

        df=pd.DataFrame(tickbuf,columns=[
            'exchange',
            'sec_id',
            'utc_time',
            'strtime',
            'last_price',
            'open',
            'high',
            'low',
            'cum_volume',
            'cum_amount',
            'cum_position',
            'delta_position',
            'position_long',
            'position_short',
            'last_volume',
            'last_amount',
            'upper_limit',
            'lower_limit',
            'settle_price',
            'trade_type',
            'pre_close',
            'bids_1_price',
            'bids_1_volume',
            #'bids_2_price',
            #'bids_2_volume',
            #'bids_3_price',
            #'bids_3_volume',
            #'bids_4_price',
            #'bids_4_volume',
            #'bids_5_price',
            #'bids_5_volume',
            'asks_1_price',
            'asks_1_volume'
           #'asks_2_price',
           #'asks_2_volume',
           #'asks_3_price',
           #'asks_3_volume',
           #'asks_4_price',
           #'asks_4_volume',
           #'asks_5_price',
           #'asks_5_volume'
            ])
        df.to_csv(DC.TICKS_DATA_PATH+symbol+'\\'+symbol+oprdate+'ticks.csv')


