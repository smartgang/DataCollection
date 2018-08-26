# -*- coding: utf-8 -*-
'''
将ricequant的数据，转换为myquant
不变：
    open
    high
    low
    close
    volume
更名：
    total_turnover->amount
    open_interest->position
增加：
    bar_type
    exchange
    sec_id
    strtime:2016-01-04T09:00:00+08:00
    strendtime:2016-01-04T09:00:00+08:00
    utc_time:1451869140
    utc_endtime:451869200`
    Unnamed: 0:0~
    adj_factor:0
    pre_close
注意：
    ricequant中的时间，表示的是k线的结束时间，相当于strendtime
    10分钟的数据14：55最后5分钟没有生成1根K线，相当于10分钟的数据不可用，需要手动合成
日线：
    不需要strendtime和utc_endtime
    settlement->settle_price
    prev_settlement->pre_close
    limit_up/limit_down ->upper_limit/lower_limit
'''
import pandas as pd
import time
from datetime import datetime
import os
import DATA_CONSTANTS as DC
import multiprocessing


# 时间格式转换
def getEndutc(ricedf):
    # print ricedf['Unnamed: 0']
    # 有些的时间列没有index 的列名，要使用'Unnamed: 0'
    return int(time.mktime(time.strptime(ricedf['Unnamed: 0'], '%Y-%m-%d %H:%M:%S')))


def getEndutcfor1d(ricedf):
    # print ricedf['Unnamed: 0']
    # 有些的时间列没有index 的列名，要使用'Unnamed: 0'
    return int(time.mktime(time.strptime(ricedf['Unnamed: 0'] + ' 00:00:00', '%Y-%m-%d %H:%M:%S')))


def getstrtime(ricedf):
    return datetime.fromtimestamp(ricedf['utc_time'], tz=None).strftime('%Y-%m-%d %H:%M:%S') + '+08:00'


def getstrendtime(ricedf):
    return datetime.fromtimestamp(ricedf['utc_endtime'], tz=None).strftime('%Y-%m-%d %H:%M:%S') + '+08:00'


def transfer(yearlist, minutelist, exchange, sec_id, filehead, datapath):
    for year in yearlist:
        for minute in minutelist:
            bar_type = 60 * minute
            print year, bar_type
            filename = filehead + "_%s_%dm.csv" % (year, minute)
            ricedf = pd.read_csv(datapath + filename)
            ricedf['utc_endtime'] = ricedf.apply(lambda t: getEndutc(t), axis=1)
            ricedf['utc_time'] = ricedf['utc_endtime'] - bar_type
            ricedf['strtime'] = ricedf.apply(lambda t: getstrtime(t), axis=1)
            ricedf['strendtime'] = ricedf.apply(lambda t: getstrendtime(t), axis=1)
            myquantdf = pd.DataFrame()
            myquantdf['strtime'] = ricedf['strtime']
            myquantdf['strendtime'] = ricedf['strendtime']
            myquantdf['utc_time'] = ricedf['utc_time']
            myquantdf['utc_endtime'] = ricedf['utc_endtime']
            myquantdf['open'] = ricedf['open']
            myquantdf['high'] = ricedf['high']
            myquantdf['low'] = ricedf['low']
            myquantdf['close'] = ricedf['close']
            myquantdf['volume'] = ricedf['volume']
            myquantdf['amount'] = ricedf['total_turnover']
            myquantdf['position'] = ricedf['open_interest']

            myquantdf['bar_type'] = bar_type
            myquantdf['exchange'] = exchange
            myquantdf['sec_id'] = sec_id
            myquantdf['Unnamed: 0'] = range(0, ricedf.shape[0])
            myquantdf['adj_factor'] = 0
            myquantdf['pre_close'] = 0
            tofilename = "%s.%s %d %s.csv" % (exchange, sec_id, bar_type, year)
            myquantdf.to_csv(datapath + 'toMyquant\\' + tofilename)


def rice1mTo10m(yearlist, minutelist, exchange, sec_id, datapath):
    '''
    将ricequant转换成myquant后的1m数据，转换成10m的数据
    主要是解决14：55分的K线问题
    前提：在原始数据中增加range列，表示该K线覆盖的范围
    :return:
    '''

    minute = minutelist[0]
    bar_type = 60 * minute
    for year in yearlist:
        fname = "%s.%s %d %s_for_10m.csv" % (exchange, sec_id, bar_type, year)
        df1m = pd.read_csv(datapath + fname)
        list10m = []
        i = 0
        Unnamed = 0
        datalen = df1m.shape[0]
        while i < datalen:
            print i
            starti = i
            endi = i + df1m.ix[i]['range']
            strtime = df1m.ix[starti]['strtime']
            strendtime = df1m.ix[endi - 1]['strendtime']
            utc_time = df1m.ix[starti]['utc_time']
            utc_endtime = df1m.ix[endi - 1]['utc_endtime']
            open = df1m.ix[starti]['open']
            high = df1m.iloc[starti:endi].high.max()
            low = df1m.iloc[starti:endi].low.min()
            close = df1m.ix[endi - 1]['close']
            volume = df1m.iloc[starti:endi].volume.sum()
            amount = df1m.iloc[starti:endi].amount.sum()
            position = df1m.ix[endi - 1]['position']
            list10m.append([strtime, strendtime, utc_time, utc_endtime, open, high, low, close, volume, amount, position, 600, exchange, sec_id, Unnamed, 0, 0])
            Unnamed += 1
            i = endi
        df10m = pd.DataFrame(list10m,
                             columns=['strtime', 'strendtime', 'utc_time', 'utc_endtime', 'open', 'high', 'low', 'close', 'volume', 'amount', 'position', 'bar_type', 'exchange',
                                      'sec_id', 'Unnamed: 0', 'adj_factor', 'pre_close'])
        tofname = "%s.%s %d %s.csv" % (exchange, sec_id, 600, year)
        df10m.to_csv(datapath + tofname)
    pass


def rice1mTo1h(yearlist, minutelist, exchange, sec_id, datapath):
    '''
    将ricequant转换成myquant后的1m数据，转换成1h的数据
    前提：
    复用10m的转换数据，在原始数据中增加range列，表示该K线10m的覆盖范围
    :return:
    '''
    minute = minutelist[0]
    bar_type = 60 * minute
    list1h = []
    for year in yearlist:
        fname = "%s.%s %d %s_for_10m.csv" % (exchange, sec_id, bar_type, year)
        df1m = pd.read_csv(datapath + fname)
        i = 0
        Unnamed = 0
        datalen = df1m.shape[0]
        range = 0
        starti = 0
        endi = 0
        while i < datalen:
            print i
            r = df1m.ix[i]['range']
            i += r
            range += r
            if range == 60 or r == 5:  # 满1小时或者到达下午收盘点
                range = 0
                endi = i
                strtime = df1m.ix[starti]['strtime']
                strendtime = df1m.ix[endi - 1]['strendtime']
                utc_time = df1m.ix[starti]['utc_time']
                utc_endtime = df1m.ix[endi - 1]['utc_endtime']
                open = df1m.ix[starti]['open']
                high = df1m.iloc[starti:endi].high.max()
                low = df1m.iloc[starti:endi].low.min()
                close = df1m.ix[endi - 1]['close']
                volume = df1m.iloc[starti:endi].volume.sum()
                amount = df1m.iloc[starti:endi].amount.sum()
                position = df1m.ix[endi - 1]['position']
                list1h.append([strtime, strendtime, utc_time, utc_endtime, open, high, low, close, volume, amount, position, 3600, exchange, sec_id, Unnamed, 0, 0])
                Unnamed += 1
                starti = i
    df1h = pd.DataFrame(list1h, columns=['strtime', 'strendtime', 'utc_time', 'utc_endtime', 'open', 'high', 'low', 'close', 'volume', 'amount', 'position', 'bar_type', 'exchange',
                                         'sec_id', 'Unnamed: 0', 'adj_factor', 'pre_close'])
    tofname = "%s.%s %d.csv" % (exchange, sec_id, 3600,)
    df1h.to_csv(datapath + tofname)
    pass


def dataconcat(yearlist, minutelist, exchange, sec_id, datapath):
    '''
    将数据进行拼接
    :return:
    '''
    for minute in minutelist:
        print minute
        df = pd.DataFrame(columns=['strtime', 'strendtime', 'utc_time', 'utc_endtime', 'open', 'high', 'low',
                                   'close', 'volume', 'amount', 'position', 'bar_type', 'exchange', 'sec_id',
                                   'Unnamed: 0', 'adj_factor', 'pre_close'])
        bar_type = minute * 60
        for year in yearlist:
            print year
            fname = "%s.%s %d %s.csv" % (exchange, sec_id, bar_type, year)
            df2 = pd.read_csv(datapath + 'toMyquant\\' + fname)
            df = pd.concat([df, df2])
        df.drop(labels='Unnamed: 0.1', axis=1, inplace=True)
        df['Unnamed: 0'] = range(0, df.shape[0])
        df = df.reset_index(drop=True)
        # df.drop('Unnamed: 0.1')
        tofile = "%s.%s %d_rice.csv" % (exchange, sec_id, bar_type)
        df.to_csv(datapath + 'toMyquant\\' + tofile)


# ==============================================================
# 2018-04-02：米筐数据转换
def riceToMyquant(ricesymbol, myquantexchange, myquantsec, minutelist, enddate, tofolder):
    print('Processing %s to %s.%s' % (ricesymbol, myquantexchange, myquantsec))
    for minute in minutelist:
        bar_type = 60 * minute
        print bar_type
        rawfolder = ("ricequant%dm\\" % minute)
        filename = rawfolder + "%s_%s_%dm.csv" % (ricesymbol, enddate, minute)
        ricedf = pd.read_csv(filename)
        ricedf['utc_endtime'] = ricedf.apply(lambda t: getEndutc(t), axis=1)
        ricedf['utc_time'] = ricedf['utc_endtime'] - bar_type
        ricedf['strtime'] = ricedf.apply(lambda t: getstrtime(t), axis=1)
        ricedf['strendtime'] = ricedf.apply(lambda t: getstrendtime(t), axis=1)
        myquantdf = pd.DataFrame()
        myquantdf['strtime'] = ricedf['strtime']
        myquantdf['strendtime'] = ricedf['strendtime']
        myquantdf['trading_date'] = ricedf['trading_date']
        myquantdf['utc_time'] = ricedf['utc_time']
        myquantdf['utc_endtime'] = ricedf['utc_endtime']
        myquantdf['open'] = ricedf['open']
        myquantdf['high'] = ricedf['high']
        myquantdf['low'] = ricedf['low']
        myquantdf['close'] = ricedf['close']
        myquantdf['volume'] = ricedf['volume']
        myquantdf['amount'] = ricedf['total_turnover']
        myquantdf['position'] = ricedf['open_interest']
        myquantdf['limit_up'] = ricedf['limit_up']
        myquantdf['limit_down'] = ricedf['limit_down']

        myquantdf['bar_type'] = bar_type
        myquantdf['exchange'] = myquantexchange
        myquantdf['sec_id'] = myquantsec
        #myquantdf['Unnamed: 0'] = range(0, ricedf.shape[0])
        myquantdf['adj_factor'] = 0
        myquantdf['pre_close'] = 0
        tofilename = "%s.%s %d_%s.csv" % (myquantexchange, myquantsec, bar_type, enddate)
        myquantdf.to_csv(tofolder + tofilename)


def ricequant1mTo10m(myquantexchange, myquantsec, enddate, datapath):
    '''
    将ricequant转换成myquant后的1m数据，转换成10m的数据
    主要是解决14：55分的K线问题
    遍历时间，如果当前时间是下午14:55，则range=5，其他时候range=10

    30分钟：
    CFFEX:直接30根对齐
    其他交易所：14:45对齐
    :return:
    '''
    print('Processing %s.%s 1m to 30m' % (myquantexchange, myquantsec))
    bar_type = 1800
    fname = "%s.%s %d_%s.csv" % (myquantexchange, myquantsec, 60, enddate)
    df1m = pd.read_csv(datapath + fname)
    list10m = []
    i = 0
    Unnamed = 0
    datalen = df1m.shape[0]
    if myquantexchange == 'CFFEX':
        aligtime = '15:15'
    else:
        aligtime = '14:45'
    while i < datalen:
        # print i
        starti = i
        strtime = df1m.ix[starti]['strtime']
        if aligtime in strtime:
            # 10分钟用5，30分钟用15
            #range10m=5
            range10m = 15
        else:
            # 10分钟用10，30分钟用30
            #range10m=10
            range10m = 30
        endi = i + range10m
        strendtime = df1m.ix[endi - 1]['strendtime']
        utc_time = df1m.ix[starti]['utc_time']
        utc_endtime = df1m.ix[endi - 1]['utc_endtime']
        tradingdate = df1m.ix[starti]['trading_date']
        open = df1m.ix[starti]['open']
        high = df1m.iloc[starti:endi].high.max()
        low = df1m.iloc[starti:endi].low.min()
        close = df1m.ix[endi - 1]['close']
        volume = df1m.iloc[starti:endi].volume.sum()
        amount = df1m.iloc[starti:endi].amount.sum()
        position = df1m.ix[endi - 1]['position']
        limit_up = df1m.ix[starti]['limit_up']
        limit_down = df1m.ix[starti]['limit_down']
        list10m.append([strtime, strendtime, utc_time, utc_endtime, tradingdate,open, high, low, close, volume, amount, position, limit_up,
                        limit_down, bar_type, myquantexchange, myquantsec, 0, 0])
        Unnamed += 1
        i = endi
    df10m = pd.DataFrame(list10m,
                         columns=['strtime', 'strendtime', 'utc_time', 'utc_endtime', 'trading_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'position', 'limit_up',
                                  'limit_down','bar_type', 'exchange','sec_id', 'adj_factor', 'pre_close'])
    tofname = "%s.%s %d_%s.csv" % (myquantexchange, myquantsec, bar_type, enddate)
    df10m.to_csv(datapath + tofname)
    pass


def ricequant1mTo1h(myquantexchange, myquantsec, enddate, datapath):
    '''
    CFFEX:15:14收盘
    其他：14：59收盘
    从头开始数，到60根或者到收盘时间，则生成一根
    :return:
    '''
    print('Processing %s.%s 1m to 60m' % (myquantexchange, myquantsec))
    bar_type = 7200
    fname = "%s.%s %d_%s.csv" % (myquantexchange, myquantsec, 60, enddate)
    df1m = pd.read_csv(datapath + fname)
    list60m = []
    i = 0
    Unnamed = 0
    datalen = df1m.shape[0]
    if myquantexchange == 'CFFEX':
        shuttime = '15:14'
    else:
        shuttime = '14:59'
    k = 0
    while i < datalen:
        # i作为遍历指针
        # k为60根的计数
        k += 1
        st = df1m.ix[i]['strtime']
        if shuttime in st or k == 120:
            starti = i - k + 1
            endi = i + 1
            strtime = df1m.ix[starti]['strtime']
            strendtime = df1m.ix[endi - 1]['strendtime']
            utc_time = df1m.ix[starti]['utc_time']
            utc_endtime = df1m.ix[endi - 1]['utc_endtime']
            open = df1m.ix[starti]['open']
            high = df1m.iloc[starti:endi].high.max()
            low = df1m.iloc[starti:endi].low.min()
            close = df1m.ix[endi - 1]['close']
            volume = df1m.iloc[starti:endi].volume.sum()
            amount = df1m.iloc[starti:endi].amount.sum()
            position = df1m.ix[endi - 1]['position']
            list60m.append([strtime, strendtime, utc_time, utc_endtime, open, high, low, close, volume, amount, position, bar_type, myquantexchange, myquantsec, Unnamed, 0, 0])
            Unnamed += 1
            k = 0
        i += 1
    df10m = pd.DataFrame(list60m,
                         columns=['strtime', 'strendtime', 'utc_time', 'utc_endtime', 'open', 'high', 'low', 'close', 'volume', 'amount', 'position', 'bar_type', 'exchange',
                                  'sec_id', 'Unnamed: 0', 'adj_factor', 'pre_close'])
    tofname = "%s.%s %d_%s.csv" % (myquantexchange, myquantsec, bar_type, enddate)
    df10m.to_csv(datapath + tofname)
    pass


def ricequant1dTo1d(ricesymbol, myquantexchange, myquantsec, enddate, tofolder):
    '''
    不需要strendtime和utc_endtime
    settlement->settle_price
    prev_settlement->pre_close
    limit_up/limit_down ->upper_limit/lower_limit
    '''
    print('Processing %s to %s.%s' % (ricesymbol, myquantexchange, myquantsec))
    bar_type = 0
    print bar_type
    rawfolder = ("ricequant1d\\")
    filename = rawfolder + "%s_%s_1d.csv" % (ricesymbol, enddate)
    ricedf = pd.read_csv(filename)
    ricedf['utc_time'] = ricedf.apply(lambda t: getEndutcfor1d(t), axis=1)
    ricedf['strtime'] = ricedf.apply(lambda t: getstrtime(t), axis=1)
    myquantdf = pd.DataFrame()
    myquantdf['strtime'] = ricedf['strtime']
    myquantdf['utc_time'] = ricedf['utc_time']
    myquantdf['open'] = ricedf['open']
    myquantdf['high'] = ricedf['high']
    myquantdf['low'] = ricedf['low']
    myquantdf['close'] = ricedf['close']
    myquantdf['volume'] = ricedf['volume']
    myquantdf['amount'] = ricedf['total_turnover']
    myquantdf['position'] = ricedf['open_interest']

    myquantdf['settle_price'] = ricedf['settlement']
    myquantdf['pre_close'] = ricedf['prev_settlement']
    myquantdf['upper_limit'] = ricedf['limit_up']
    myquantdf['lower_limit'] = ricedf['limit_down']

    myquantdf['bar_type'] = bar_type
    myquantdf['exchange'] = myquantexchange
    myquantdf['sec_id'] = myquantsec
    myquantdf['Unnamed: 0'] = range(0, ricedf.shape[0])
    myquantdf['adj_factor'] = 0

    tofilename = "%s.%s %d_%s.csv" % (myquantexchange, myquantsec, bar_type, enddate)
    myquantdf.to_csv(tofolder + tofilename)


def riceGenerateBar(bar_type):
    # 使用米筐的数据重新生成bar
    # 总共有2份:2016-05-01和2018-05-30
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    symbols = pd.read_excel('rice_symbol_map_temp.xlsx')['symbol']
    for symbol in symbols:
        print symbol, bar_type
        ricefolder = "riceToMyquant %s\\" % (symbol)
        ricedata1 = pd.read_csv(ricefolder + "%s %d_%s.csv" % (symbol, bar_type, '2016-05-01'))
        ricedata2 = pd.read_csv(ricefolder + "%s %d_%s.csv" % (symbol, bar_type, '2018-05-30'))
        startutc = float(time.mktime(time.strptime('2016-05-01' + " 00:00:00", "%Y-%m-%d %H:%M:%S")))
        taildf = ricedata2.loc[(ricedata2['utc_time'] >= startutc)]
        bardata = pd.concat([ricedata1, taildf])
        tofilename = ("%s %d.csv" % (symbol, bar_type))
        tofolder = ("D:\\002 MakeLive\DataCollection\\bar data\\%s" % symbol)
        try:
            os.mkdir(tofolder)
        except:
            pass
        bardata.reset_index(drop=True, inplace=True)
        bardata = bardata.drop('Unnamed: 0', axis=1)
        bardata.to_csv(DC.BAR_DATA_PATH + symbol + '\\' + tofilename)
    pass

def riceConcateToBar(bar_type, enddate):
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    contractlist = pd.read_excel(DC.PUBLIC_DATA_PATH + 'Contract.xlsx')['Contract']
    for symbol in contractlist:
        print symbol
        ricefolder = "riceToMyquant %s\\" % (symbol)
        ricedata = pd.read_csv(ricefolder + "%s %d_%s.csv" % (symbol, bar_type, enddate))
        bardata = DC.getBarData(symbol, bar_type, starttime='2016-05-01 00:00:00', endtime='2018-03-31 23:59:59')
        bardata = pd.concat([ricedata, bardata])
        tofilename = ("%s %d.csv" % (symbol, bar_type))
        bardata.reset_index(drop=True, inplace=True)
        bardata = bardata.drop('Unnamed: 0', axis=1)
        bardata = bardata.drop('Unnamed: 0.1', axis=1)
        bardata.to_csv(DC.BAR_DATA_PATH + symbol + '\\' + tofilename)
    pass


if __name__ == '__main__':
    '''
    yearlist = ['2010','2011','2012','2013', '2014', '2015', '2016','2017']
    exchange = 'SHFE'
    sec_id = 'RB'
    minutelist = [1]
    datapath='D:\\002 MakeLive\DataCollection\\ricequant data\\RB\\'
    filehead='rbdata'
    #transfer(yearlist,minutelist,exchange,sec_id,filehead,datapath)
    rice1mTo1h(yearlist,minutelist,exchange,sec_id,datapath)
    #minutelist=[10]
    #dataconcat(yearlist,minutelist,exchange,sec_id,datapath)
    '''
    '''
    #1,5,15分钟转换
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    enddate='2018-06-07'
    minutelist = [1]
    symbolmap=pd.read_excel('rice_symbol_map_temp2.xlsx')
    symbolnum=symbolmap.shape[0]
    for i in range(symbolnum):
        ricesymbol=symbolmap.ix[i,'order_book_id']
        myquantexchange=symbolmap.ix[i,'exchange']
        myquantsec = symbolmap.ix[i,'sec']
        tofolder="riceToMyquant %s.%s"%(myquantexchange,myquantsec)
        try:
            os.mkdir(tofolder)
        except:
            print('folder exist!')
        riceToMyquant(ricesymbol,myquantexchange,myquantsec,minutelist,enddate,tofolder+'\\')
    '''
    '''
    #1分钟转为10分钟
    #1分钟转为30分钟
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    enddate='2018-05-30'
    symbolmap=pd.read_excel('rice_symbol_map.xlsx')
    symbolnum=symbolmap.shape[0]
    pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
    for i in range(symbolnum):
        ricesymbol=symbolmap.ix[i,'order_book_id']
        myquantexchange=symbolmap.ix[i,'exchange']
        myquantsec = symbolmap.ix[i,'sec']
        tofolder="riceToMyquant %s.%s"%(myquantexchange,myquantsec)
        try:
            os.mkdir(tofolder)
        except:
            print('folder exist!')

        #ricequant1mTo10m(myquantexchange,myquantsec,enddate,tofolder+'\\')
        #ricequant1mTo1h(myquantexchange,myquantsec,enddate,tofolder+'\\')
        pool.apply_async(ricequant1mTo10m, (myquantexchange,myquantsec,enddate,tofolder+'\\'))
    pool.close()
    pool.join()
    '''
    '''
    # 日线转换
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    enddate = '2016-05-01'
    symbolmap = pd.read_excel('rice_symbol_map.xlsx')
    symbolnum = symbolmap.shape[0]
    for i in range(symbolnum):
        ricesymbol = symbolmap.ix[i, 'order_book_id']
        myquantexchange = symbolmap.ix[i, 'exchange']
        myquantsec = symbolmap.ix[i, 'sec']
        tofolder = "riceToMyquant %s.%s" % (myquantexchange, myquantsec)
        try:
            os.mkdir(tofolder)
        except:
            print('folder exist!')
            ricequant1dTo1d(ricesymbol, myquantexchange, myquantsec, enddate, tofolder + '\\')

    # 将米筐拼接到bar中
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    enddate = '2016-05-01'
    barlist = [0]
    for bartype in barlist:
        riceConcateToBar(bartype, enddate)
    '''

    # 将米筐数据重新合成bar
    barlist = [60]
    for bar in barlist:
        riceGenerateBar(bar)
