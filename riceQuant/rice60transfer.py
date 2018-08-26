# -*- coding: utf-8 -*-
# 米筐小时数据整理
import pandas as pd
import os
import time
from copyToBar import copyToBar

def generatNightAdj(symbol):
    # 生成夜盘调整标识文件
    print symbol
    folder ="D:\\002 MakeLive\DataCollection\\bar data\\"+symbol
    rawfilename = symbol+' 60.csv'
    rawdata = pd.read_csv(folder+'\\'+rawfilename)
    knum = rawdata.shape[0]
    rawdata['k_index'] = range(knum)
    grouped = rawdata.groupby(rawdata['trading_date'])
    firstindex = grouped['k_index'].first()
    lastindex = grouped['k_index'].last()
    knum = (lastindex - firstindex)+1
    knum_drop=knum.drop_duplicates()
    kdf = pd.DataFrame(knum)
    kdf.rename(columns={"k_index":"real"},inplace=True)
    kdf['total'] = knum_drop
    kdf.fillna(method='ffill',inplace=True)
    kdf['total'] = kdf.max(axis=1)
    kdf['night'] = kdf['total'] - kdf['real']
    kdf['isNight'] = False
    kdf.loc[kdf['real'] >= kdf['total'], 'isNight'] = True  # 米筐有时候会在按变换夜盘之前的时间，插一下假数据，导致real>total
    kdf['adjust'] = kdf['total'] % 60
    kdf['night_adj'] = kdf['night'] % 60
    kdf.to_csv('riceToMyquant '+symbol+'\\'+symbol+'60adj.csv')


def generatNightAdj2(symbol):
    # 适配riceContractDataTransfer2
    # 生成夜盘调整标识文件
    print symbol
    folder ="D:\\002 MakeLive\DataCollection\\bar data\\"+symbol
    rawfilename = symbol+' 60.csv'
    rawdata = pd.read_csv(folder+'\\'+rawfilename)
    knum = rawdata.shape[0]
    rawdata['k_index'] = range(knum)
    grouped = rawdata.groupby(rawdata['trading_date'])
    firstindex = grouped['k_index'].first()
    lastindex = grouped['k_index'].last()
    knum = (lastindex - firstindex)+1
    knum_drop=knum.drop_duplicates()
    kdf = pd.DataFrame(knum)
    kdf.rename(columns={"k_index":"real"},inplace=True)
    kdf['total'] = knum_drop
    kdf.fillna(method='ffill',inplace=True)
    kdf['total'] = kdf.max(axis=1)
    kdf['night'] = kdf['total'] - kdf['real']
    kdf['isNight'] = False
    kdf.loc[kdf['real'] >= kdf['total'], 'isNight'] = True  # 米筐有时候会在按变换夜盘之前的时间，插一下假数据，导致real>total
    kdf['adjust'] = kdf['total'] % 60
    kdf['night_adj'] = kdf['night'] % 60
    kdf.to_csv('rqdata_raw_'+symbol+'\\'+symbol+'60adj.csv')


def rice60transfer(symbol, enddate):
    print symbol
    exchange, sec = symbol.split('.')
    folder = 'riceToMyquant '+symbol
    rawfilename = symbol+' 60_'+enddate+'.csv'
    rawdata = pd.read_csv(folder+'\\'+rawfilename)
    njghtadjdf = pd.read_csv(folder+'\\'+symbol+'60adj.csv', index_col='trading_date')
    bar_type = 3600
    list60m = []
    i = 0
    datalen = rawdata.shape[0]
    lastdate = rawdata.ix[0]['trading_date']
    nightajd = njghtadjdf.ix[lastdate, 'night_adj']
    dayadj = njghtadjdf.ix[lastdate,'adjust']
    if exchange != 'CFFEX':
        if dayadj == 45:  # 对于没有夜盘且正常夜盘为非整点的情况
            aligtime = '14:15'
        else:
            aligtime = '14:45'  # 有夜盘，或者没有夜盘但是夜盘为整点的情况
    else:
        aligtime = '15:15'

    while i < datalen:
        starti = i
        strtime = rawdata.ix[starti]['strtime']
        tradingdate = rawdata.ix[starti]['trading_date']
        if tradingdate != lastdate:  # 每换一天判断一次
            firstInDay = True
            nightajd = njghtadjdf.ix[tradingdate, 'night_adj']
            dayadj = njghtadjdf.ix[tradingdate, 'adjust']
            if exchange != 'CFFEX':
                if dayadj == 45:  # 最后一根K线有45根，所以从14：15开始
                    aligtime = '14:15'
                else:
                    aligtime = '14:45'  # 不是45就是15，那就从14:45开始
            else:
                if dayadj == 30:
                    aligtime = '14:45'
                else:
                    aligtime = '15:30'
        else:
            firstInDay = False
        if exchange != 'CFFEX':
            if firstInDay and nightajd == 30:  # 如果是当天第一根，而且需要做调整，说明第1根只能取30根1分钟线
                range60m = 30
            elif aligtime in strtime:
                range60m = int(dayadj)
            else:
                range60m = 60
        else:
            if aligtime in strtime:
                range60m = int(dayadj)
            else:
                range60m = 60
        endi = i + range60m
        strendtime = rawdata.ix[endi - 1]['strendtime']
        utc_time = rawdata.ix[starti]['utc_time']
        utc_endtime = rawdata.ix[endi - 1]['utc_endtime']
        #tradingdate = rawdata.ix[starti]['trading_date']
        open = rawdata.ix[starti]['open']
        high = rawdata.iloc[starti:endi].high.max()
        low = rawdata.iloc[starti:endi].low.min()
        close = rawdata.ix[endi - 1]['close']
        volume = rawdata.iloc[starti:endi].volume.sum()
        amount = rawdata.iloc[starti:endi].amount.sum()
        position = rawdata.ix[endi - 1]['position']
        limit_up = rawdata.ix[starti]['limit_up']
        limit_down = rawdata.ix[starti]['limit_down']
        list60m.append([strtime, strendtime, utc_time, utc_endtime, tradingdate,open, high, low, close, volume, amount, position, limit_up,
                        limit_down, bar_type, exchange, sec, 0, 0])
        i = endi
        lastdate = tradingdate
    df10m = pd.DataFrame(list60m,
                         columns=['strtime', 'strendtime', 'utc_time', 'utc_endtime', 'trading_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'position', 'limit_up',
                                  'limit_down','bar_type', 'exchange','sec_id', 'adj_factor', 'pre_close'])
    tofname = "%s.%s %d_%s.csv" % (exchange, sec, bar_type, enddate)
    df10m.to_csv(folder+'\\' + tofname)


def transfer1mTo60m(symbollist, domain_symbol, enddate):
    """
    生成1小时数据
    ！！使用前提！！：
        更新主连数据
        在rice60transfer中使用更新后的主连数据，更新60adj文件
    :return:
    """
    rawfolder = ("riceToMyquant %s\\" % domain_symbol)
    exchange, sec = domain_symbol.split('.')
    bar_type = 3600
    for symbol in symbollist:
        print('Processing %s bartype %d %s ' % (symbol, bar_type, enddate))
        fname = "%s 60_%s.csv" % (symbol, enddate)
        rawdata = pd.read_csv(rawfolder + fname)
        njghtadjdf = pd.read_csv(rawfolder+'\\'+domain_symbol+'60adj.csv', index_col='trading_date')
        list60m = []
        i = 0
        datalen = rawdata.shape[0]
        lastdate = rawdata.ix[0]['trading_date']
        nightajd = njghtadjdf.ix[lastdate, 'night_adj']
        dayadj = njghtadjdf.ix[lastdate,'adjust']
        if exchange != 'CFFEX':
            if dayadj == 45:  # 对于没有夜盘且正常夜盘为非整点的情况
                aligtime = '14:15'
            else:
                aligtime = '14:45'  # 有夜盘，或者没有夜盘但是夜盘为整点的情况
        else:
            aligtime = '15:15'

        while i < datalen:
            starti = i
            strtime = rawdata.ix[starti]['strtime']
            tradingdate = rawdata.ix[starti]['trading_date']
            if tradingdate != lastdate:  # 每换一天判断一次
                firstInDay = True
                nightajd = njghtadjdf.ix[tradingdate, 'night_adj']
                dayadj = njghtadjdf.ix[tradingdate, 'adjust']
                if exchange != 'CFFEX':
                    if dayadj == 45:  # 最后一根K线有45根，所以从14：15开始
                        aligtime = '14:15'
                    else:
                        aligtime = '14:45'  # 不是45就是15，那就从14:45开始
                else:
                    if dayadj == 30:
                        aligtime = '14:45'
                    else:
                        aligtime = '15:30'
            else:
                firstInDay = False
            if exchange != 'CFFEX':
                if firstInDay and nightajd == 30:  # 如果是当天第一根，而且需要做调整，说明第1根只能取30根1分钟线
                    range60m = 30
                elif aligtime in strtime:
                    range60m = int(dayadj)
                else:
                    range60m = 60
            else:
                if aligtime in strtime:
                    range60m = int(dayadj)
                else:
                    range60m = 60
            endi = i + range60m
            if endi > datalen:
                break  # 最后一小时可能不会合满，在这里要做一个保护
            strendtime = rawdata.ix[endi - 1]['strendtime']
            utc_time = rawdata.ix[starti]['utc_time']
            utc_endtime = rawdata.ix[endi - 1]['utc_endtime']
            #tradingdate = rawdata.ix[starti]['trading_date']
            open = rawdata.ix[starti]['open']
            high = rawdata.iloc[starti:endi].high.max()
            low = rawdata.iloc[starti:endi].low.min()
            close = rawdata.ix[endi - 1]['close']
            volume = rawdata.iloc[starti:endi].volume.sum()
            amount = rawdata.iloc[starti:endi].amount.sum()
            position = rawdata.ix[endi - 1]['position']
            limit_up = rawdata.ix[starti]['limit_up']
            limit_down = rawdata.ix[starti]['limit_down']
            list60m.append([strtime, strendtime, utc_time, utc_endtime, open, high, low, close, volume, amount, position, limit_up,
                            limit_down, 0, 0, tradingdate, exchange, sec, symbol, bar_type])
            i = endi
            lastdate = tradingdate
        df60m = pd.DataFrame(list60m,
                             columns=['strtime', 'strendtime', 'utc_time', 'utc_endtime', 'open', 'high', 'low', 'close', 'volume', 'amount', 'position', 'limit_up',
                                               'limit_down', 'adj_factor', 'pre_close', 'trading_date', 'exchange', 'sec_id', 'symbol', 'bar_type'])
        tofname = "%s %d_%s.csv" % (symbol, bar_type, enddate)
        df60m.to_csv(rawfolder + tofname, index=False)


def transfer1mTo60m2(symbollist, domain_symbol):
    """
    适配riceContractDataTransfer2
    生成1小时数据
    ！！使用前提！！：
        更新主连数据
        在rice60transfer中使用更新后的主连数据，更新60adj文件
    :return:
    """
    rawfolder = ("rqdata_raw_%s\\" % domain_symbol)
    exchange, sec = domain_symbol.split('.')
    bar_type = 3600
    for symbol in symbollist:
        print('Processing %s bartype %d' % (symbol, bar_type))
        fname = "%s 60.csv" % symbol
        rawdata = pd.read_csv(rawfolder + fname)
        njghtadjdf = pd.read_csv(rawfolder+'\\'+domain_symbol+'60adj.csv', index_col='trading_date')
        list60m = []
        i = 0
        datalen = rawdata.shape[0]
        lastdate = rawdata.ix[0]['trading_date']
        nightajd = njghtadjdf.ix[lastdate, 'night_adj']
        dayadj = njghtadjdf.ix[lastdate,'adjust']
        if exchange != 'CFFEX':
            if dayadj == 45:  # 对于没有夜盘且正常夜盘为非整点的情况
                aligtime = '14:15'
            else:
                aligtime = '14:45'  # 有夜盘，或者没有夜盘但是夜盘为整点的情况
        else:
            aligtime = '15:15'

        while i < datalen:
            starti = i
            strtime = rawdata.ix[starti]['strtime']
            tradingdate = rawdata.ix[starti]['trading_date']
            if tradingdate != lastdate:  # 每换一天判断一次
                firstInDay = True
                nightajd = njghtadjdf.ix[tradingdate, 'night_adj']
                dayadj = njghtadjdf.ix[tradingdate, 'adjust']
                if exchange != 'CFFEX':
                    if dayadj == 45:  # 最后一根K线有45根，所以从14：15开始
                        aligtime = '14:15'
                    else:
                        aligtime = '14:45'  # 不是45就是15，那就从14:45开始
                else:
                    if dayadj == 30:
                        aligtime = '14:45'
                    else:
                        aligtime = '15:30'
            else:
                firstInDay = False
            if exchange != 'CFFEX':
                if firstInDay and nightajd == 30:  # 如果是当天第一根，而且需要做调整，说明第1根只能取30根1分钟线
                    range60m = 30
                elif aligtime in strtime:
                    range60m = int(dayadj)
                else:
                    range60m = 60
            else:
                if aligtime in strtime:
                    range60m = int(dayadj)
                else:
                    range60m = 60
            endi = i + range60m
            if endi > datalen:
                break  # 最后一小时可能不会合满，在这里要做一个保护
            strendtime = rawdata.ix[endi - 1]['strendtime']
            utc_time = rawdata.ix[starti]['utc_time']
            utc_endtime = rawdata.ix[endi - 1]['utc_endtime']
            #tradingdate = rawdata.ix[starti]['trading_date']
            open = rawdata.ix[starti]['open']
            high = rawdata.iloc[starti:endi].high.max()
            low = rawdata.iloc[starti:endi].low.min()
            close = rawdata.ix[endi - 1]['close']
            volume = rawdata.iloc[starti:endi].volume.sum()
            amount = rawdata.iloc[starti:endi].amount.sum()
            position = rawdata.ix[endi - 1]['position']
            limit_up = rawdata.ix[starti]['limit_up']
            limit_down = rawdata.ix[starti]['limit_down']
            list60m.append([strtime, strendtime, utc_time, utc_endtime, open, high, low, close, volume, amount, position, limit_up,
                            limit_down, 0, 0, tradingdate, exchange, sec, symbol, bar_type])
            i = endi
            lastdate = tradingdate
        df60m = pd.DataFrame(list60m,
                             columns=['strtime', 'strendtime', 'utc_time', 'utc_endtime', 'open', 'high', 'low', 'close', 'volume', 'amount', 'position', 'limit_up',
                                               'limit_down', 'adj_factor', 'pre_close', 'trading_date', 'exchange', 'sec_id', 'symbol', 'bar_type'])
        tofname = "%s %d.csv" % (symbol, bar_type)
        df60m.to_csv(rawfolder + tofname, index=False)


if __name__ == "__main__":
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    month_mode = False  # 为True表示是月度更新模式，则只更新该月涉及的合约
    if month_mode:
        startdate = '2018-06-01'
        enddate = '2018-07-01'
        domain_map = pd.read_excel('D:\\002 MakeLive\DataCollection\public data\\domainMap.xlsx')
        contract_map = pd.read_csv('D:\\002 MakeLive\DataCollection\public data\\contractMap.csv')
        active_domain_list = domain_map.loc[domain_map['active'] == True]['symbol'].tolist()
        start_utc = int(time.mktime(time.strptime(startdate + ' 00:00:00', '%Y-%m-%d %H:%M:%S')))
        for domain_symbol in active_domain_list:
            generatNightAdj(domain_symbol)  # 生成60adj文件
            symbol_contract_map = contract_map.loc[contract_map['domain_symbol'] == domain_symbol]
            modify_symbol = symbol_contract_map.loc[(symbol_contract_map['domain_start_utc'] > start_utc) | (symbol_contract_map['domain_end_utc'] > start_utc)]
            idlist = modify_symbol['symbol'].tolist()
            transfer1mTo60m(idlist, domain_symbol, enddate)     # 生成合约的3600文件
            copyToBar(idlist, domain_symbol, enddate, [3600])   # 将3600文件拷到bar文件珍闻
    else:
        enddate = '2018-07-28'
        # symbollist = pd.read_excel('rice_symbol_map_temp1.xlsx')['symbol'].tolist()
        symbollist = ['DCE.J']
        # 每月开始处理1小时数据前，要先更新完bar里面的1分钟数据，下面是根据1分钟数据更新adj文件
        for symbol in symbollist:
            generatNightAdj(symbol)
        symboldf = pd.read_csv('D:\\002 MakeLive\DataCollection\public data\\contractMap.csv')
        for dsymbol in symbollist:
            rbdf = symboldf.loc[symboldf['domain_symbol'] == dsymbol]
            #idlist = rbdf['symbol'].tolist()
            idlist = ['J1809']
            transfer1mTo60m(idlist, dsymbol, enddate)
            copyToBar(idlist, dsymbol, enddate, [3600])
