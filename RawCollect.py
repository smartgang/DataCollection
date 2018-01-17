# -*- coding: utf-8 -*-
import datetime
from gmsdk import *
import pandas as pd
import DATA_CONSTANTS as DC

def rawCollectByDay(K_MIN_set,contractlist):
    for K_MIN in K_MIN_set:
        for c in contractlist:
            exchange_id,sec_id=c.split('.',1)
            symbol=exchange_id+'.'+sec_id
            tradedates=md.get_calendar(exchange_id, "2017-11-09", "2017-12-05")
            for td in tradedates:
                oprdate=td.strtime[0:10]              ## 交易日

                print symbol+' '+str(K_MIN)+' '+oprdate
                starttime=oprdate+' 00:00:00'
                endtime=oprdate+' 23:59:59'
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
                df.to_csv(DC.RAW_DATA_PATH+symbol+'\\'+symbol+oprdate+' '+str(K_MIN)+'.csv')
                print symbol+oprdate+' raw data collection finished!'

def rawCollectByPeriod(startdate,enddate,K_MIN_set,contractlist):
    starttime = startdate + " 00:00:00"
    endtime = enddate + " 23:59:59"
    for K_MIN in K_MIN_set:
        for c in contractlist:
            exchange_id, sec_id = c.split('.', 1)
            symbol = exchange_id + '.' + sec_id
            nextFlag = False
            databuf = []
            while (nextFlag is not True):
                bars = md.get_bars(symbol, K_MIN, starttime, endtime)
                for bar in bars:
                    databuf.append([
                        bar.exchange,  ## 交易所代码
                        bar.sec_id,  ## 证券ID
                        bar.bar_type,  ## bar类型，以秒为单位，比如1分钟bar, bar_type=60
                        bar.strtime,  ## Bar开始时间
                        bar.utc_time,  ## Bar开始时间
                        bar.strendtime,  ## Bar结束时间
                        bar.utc_endtime,  ## Bar结束时间
                        bar.open,  ## 开盘价
                        bar.high,  ## 最高价
                        bar.low,  ## 最低价
                        bar.close,  ## 收盘价
                        bar.volume,  ## 成交量
                        bar.amount,  ## 成交额
                        bar.pre_close,  ## 前收盘价
                        bar.position,  ## 持仓量
                        bar.adj_factor,  ## 复权因子
                        bar.flag])  ## 除权出息标记
                if len(databuf) == 33000:
                    starttime = datetime.datetime.fromtimestamp(databuf[-1][6]).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    nextFlag = True
            df = pd.DataFrame(databuf, columns=[
                'exchange',
                'sec_id',
                'bar_type',
                'strtime',
                'utc_time',
                'strendtime',
                'utc_endtime',
                'open', 'high', 'low', 'close', 'volume', 'amount', 'pre_close', 'position', 'adj_factor', 'flag'])
            df.to_csv(DC.RAW_DATA_PATH + symbol + '\\' + symbol + enddate + ' ' + str(K_MIN) + '.csv')
            print symbol + enddate + ' raw data collection finished!'
    pass

if __name__ == '__main__':
    K_MIN_set = [600]
    #K_MIN_set = [1800,3600]
    md.init(username="smartgang@126.com", password="39314656a")
    contractlist = pd.read_excel(DC.PUBLIC_DATA_PATH + 'Contract.xlsx')['Contract']
    startdate = "2016-01-01"
    enddate = "2017-12-30"
    rawCollectByPeriod(startdate,enddate,K_MIN_set,contractlist)