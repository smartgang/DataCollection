# -*- coding: utf-8 -*-
import datetime
from gmsdk import *
import pandas as pd
import DATA_CONSTANTS as DC

K_MIN_set=[60,300,600,900]
K_MIN=60
exchange_id='DCE'
sec_id='i1801'
md.init(username="smartgang@126.com", password="39314656a")
contractlist=pd.read_excel(DC.PUBLIC_DATA_PATH+'Contract.xlsx')['Contract']
symbol=exchange_id+'.'+sec_id
starttime="2017-11-01 00:00:00"
endtime="2017-11-10 09:51:00"
nextFlag=False
databuf=[]
while(nextFlag is not True):
    bars = md.get_bars(symbol, K_MIN, starttime, endtime)
    for bar in bars:
        databuf.append([
            bar.exchange, ## 交易所代码
            bar.sec_id , ## 证券ID
            bar.bar_type,  ## bar类型，以秒为单位，比如1分钟bar, bar_type=60
            bar.strtime ,  ## Bar开始时间
            bar.utc_time,  ## Bar开始时间
            bar.strendtime,  ## Bar结束时间
            bar.utc_endtime, ## Bar结束时间
            bar.open , ## 开盘价
            bar.high , ## 最高价
            bar.low , ## 最低价
            bar.close, ## 收盘价
            bar.volume, ## 成交量
            bar.amount,  ## 成交额
            bar.pre_close,  ## 前收盘价
            bar.position, ## 持仓量
            bar.adj_factor,  ## 复权因子
            bar.flag])  ## 除权出息标记
    if len(databuf)==33000:
        starttime=datetime.datetime.fromtimestamp(databuf[-1][6]).strftime('%Y-%m-%d %H:%M:%S')
    else:nextFlag=True
df = pd.DataFrame(databuf, columns=[
    'exchange',
    'sec_id',
    'bar_type',
    'strtime',
    'utc_time',
    'strendtime',
    'utc_endtime',
    'open','high','low','close','volume','amount','pre_close','position','adj_factor','flag'])
df.to_csv(sec_id+'_'+str(K_MIN)+'.csv')
print symbol+' raw data collection finished!'

