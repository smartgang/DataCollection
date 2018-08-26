# -*- coding: utf-8 -*-
import datetime
import pandas as pd
import DATA_CONSTANTS as DC

########################################################################
class VtBarData():
    """K线数据"""
    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.symbol = ''  # 代码
        self.exchange = ''  # 交易所
        self.bar_type = 0
        self.strtime = None
        self.utctime = 0
        self.open = 0.0  # OHLC
        self.high = 0.0
        self.low = 0.0
        self.close = 0.0
        self.volume = 0  # 成交量
        self.amount = 0 #成交额
        self.position = 0  # 持仓量


K_MIN=15

combine_offset = abs(K_MIN - 15)
symbol='CFFEX.IC'
datapath = DC.RAW_DATA_PATH + symbol + '\\'
mindata=pd.read_csv(datapath+symbol+' '+str(60)+'.csv')
rownum=mindata.shape[0]
xminlist=[]
xminBar=None
for i in range(rownum):
    bar=mindata.iloc[i]
    dt = datetime.datetime.fromtimestamp(bar.utc_time)
    if not xminBar:
        xminBar = VtBarData()
        xminBar.symbol = bar.sec_id
        xminBar.exchange = bar.exchange
        xminBar.bar_type = K_MIN*60

        xminBar.open = bar.open
        xminBar.high = bar.high
        xminBar.low = bar.low
        # 生成上一X分钟K线的时间戳
        xminBar.strtime = dt.strftime("%Y-%m-%d %H:%M:%S")
        xminBar.utctime = bar.utc_time

    # 累加老K线
    else:
        xminBar.high = max(xminBar.high, bar.high)
        xminBar.low = min(xminBar.low, bar.low)

    xminBar.close = bar.close
    xminBar.position = bar.position
    xminBar.volume += float(bar.volume)
    xminBar.amount += bar.amount

    minute = dt.minute
    timestamp = dt.hour * 60 + minute
    if timestamp == 540:  # 9:00
        combindflag = 0
    elif timestamp >= 630 and timestamp < 899:  # 10:30~15:58
        combindflag = (minute + combine_offset) % K_MIN
    elif timestamp == 899:  # 15:59
        combindflag = K_MIN - 1
    else:
        combindflag = minute % K_MIN

    # X分钟已经走完
    if combindflag == K_MIN - 1:
        # 推送
        xminlist.append([
                            xminBar.symbol,
                            xminBar.exchange,# 交易所
                            xminBar.bar_type,
                            xminBar.strtime,
                            xminBar.utctime,
                            xminBar.open,  # OHLC
                            xminBar.high ,
                            xminBar.low,
                            xminBar.close,
                            xminBar.volume,  # 成交量
                            xminBar.amount, #成交额
                            xminBar.position]) # 持仓量
        # 清空老K线缓存对象
        xminBar = None

df=pd.DataFrame(xminlist,columns=[
                                'sec_id',
                                'exchange',
                                'bar_type',
                                'strtime',
                                'utctime',
                                'open',
                                'high',
                                'low',
                                'close',
                                'volume',
                                'amount',
                                'position'
                                  ])
df.to_csv(datapath+symbol+' '+str(K_MIN*60)+'.csv')
del df
del xminlist