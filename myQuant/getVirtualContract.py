# -*- coding: utf-8 -*-
'''
获取所有品种的主力合约代码
制成品种的主力合约切换时刻表
1、获取品种的主力合约，保存为symbol+virtualContract.csv
2、制作主力合约切换表，保存为symbol+ContractSwap.csv
csv格式：
    Symbol oldcontract olddate newcontract newdate swaputc
'''
#获取主力合约代码
from gmsdk import *
import pandas as pd
import DATA_CONSTANTS as DC
import time
import os

def updateContractSwap(enddate):
    '''
    每月列新主力合约列表和主力合约切换列表
    :return:
    '''
    contractlist=pd.read_excel(DC.PUBLIC_DATA_PATH+'Contract.xlsx')['Contract']
    md.init(username="smartgang@126.com", password="39314656a")
    for contract in contractlist:
        oriContractDf = pd.read_csv(DC.Collection_Path + "vitualContract\\" + contract + 'VirtualContract.csv')
        oriContractDf.drop('Unnamed: 0',axis=1,inplace=True)
        oriSwapDf=pd.read_csv(DC.Collection_Path +'vitualContract\\'+contract+'ContractSwap.csv')
        oriSwapDf.drop('Unnamed: 0', axis=1, inplace=True)
        lastsymbol = oriContractDf.iloc[-1]
        startdate= oriContractDf.iloc[-1].trade_date
        oriContractNum = oriContractDf.shape[0]
        vsymbols=md.get_virtual_contract(contract, startdate,enddate)
        swaplist = []
        i=0
        for symbol in vsymbols:
            s=[
                symbol.vsymbol,  ##主力合约或连接合约代码
                symbol.symbol , ##真实symbol
                symbol.trade_date,  ##交易日
            ]
            oriContractDf.loc[oriContractNum+i]=s
            if symbol.symbol!=lastsymbol.symbol:
                dt = time.strptime(symbol.trade_date + ' 06:00:00', '%Y-%m-%d %H:%M:%S')
                swaputc = int(time.mktime(dt))
                swaplist.append([symbol.vsymbol, lastsymbol.symbol, lastsymbol.trade_date, symbol.symbol, symbol.trade_date,
                     swaputc])
            i+=1
            lastsymbol=symbol

        oriContractDf.to_csv(DC.Collection_Path+'vitualContract\\' + contract + 'VirtualContract.csv')
        if len(swaplist)>0:
            swapdf=pd.DataFrame(swaplist,columns=['Symbol','oldContract','oldDate','newContract','newDate','swaputc'])
            newswapdf=pd.concat([oriSwapDf,swapdf])
            newswapdf.reset_index(inplace=True,drop=True)
            newswapdf.to_csv(DC.Collection_Path+'vitualContract\\'+contract+'ContractSwap.csv')

def init():
    contractlist=pd.read_excel(DC.PUBLIC_DATA_PATH+'Contract.xlsx')['Contract']
    md.init(username="smartgang@126.com", password="39314656a")
    for contract in contractlist:
        vsymbols=md.get_virtual_contract(contract, "2016-01-01","2017-12-05")
        swaplist = []
        s=[]
        lastsymbol=vsymbols[0]
        for symbol in vsymbols:
            s.append([
                    symbol.vsymbol,  ##主力合约或连接合约代码
                    symbol.symbol , ##真实symbol
                    symbol.trade_date,  ##交易日
            ])
            if symbol.symbol != lastsymbol.symbol:
                dt = time.strptime(symbol.trade_date + ' 06:00:00', '%Y-%m-%d %H:%M:%S')
                swaputc=int(time.mktime(dt))
                swaplist.append([symbol.vsymbol,lastsymbol.symbol,lastsymbol.trade_date,symbol.symbol,symbol.trade_date,swaputc])
            lastsymbol=symbol
        contractdf=pd.DataFrame(s,columns=['vsymbol','symbol','trade_date'])
        contractdf.to_csv('vitualContract\\'+contract+'VirtualContract.csv')

        swapdf=pd.DataFrame(swaplist,columns=['Symbol','oldContract','oldDate','newContract','newDate','swaputc'])
        swapdf.to_csv('vitualContract\\'+contract+'ContractSwap.csv')

def findSwap(filedf):
    #Symbol	oldContract	oldDate	newContract	newDate	swaputc
    df=pd.DataFrame()
    df['Symbol']=filedf.vsymbol
    df['oldContract']=filedf.symbol
    df['oldDate']=filedf.trade_date
    df['newContract']=filedf.symbol.shift(-1)
    df['newDate'] = filedf.trade_date.shift(-1)
    df.dropna(inplace=True)
    swaploc=df.loc[df['oldContract']!=df['newContract']]
    if swaploc.shape[0]==0:
        return None
    swaploc.reset_index(inplace=True,drop=True)
    #swaploc.drop([swaploc.shape[0]-1],inplace=True)
    def getutc(ricedf):
        #return int(time.mktime(time.strptime(pd.datetime.strftime(ricedf['newDate']+' 00:00:00','%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S'))) - 10800
        return int(time.mktime(time.strptime(ricedf['newDate']+' 00:00:00', '%Y\%m\%d %H:%M:%S'))) - 10800  # 10800:3小时，切换时间设置为前一天的晚上21点，即交易日开始时间
        #return int(time.mktime(ricedf['newDate'] ))+21600
    swaploc['swaputc'] = swaploc.apply(lambda t: getutc(t), axis=1)
    return swaploc
    #swaploc.to_csv('D:\\002 MakeLive\DataCollection\\vitualContract\SHFE.RBContractSwap.csv')

def ricequantVsymbol():
    #制作米筐每个品种的主力合约时间表
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    symbolmap = pd.read_excel('rice_symbol_map.xlsx')
    symbolnum=symbolmap.shape[0]
    vsymbolmap = pd.read_csv("domian_contract\\domain_symbol.csv")
    vsymbolmap.fillna('None',inplace=True)
    for i in range(symbolnum):
        ricesymbol=symbolmap.ix[i,'book_id']
        myquantexchange=symbolmap.ix[i,'exchange']
        myquantsec = symbolmap.ix[i,'sec']
        if vsymbolmap.ix[0,ricesymbol] == 'None':
            nonenum=vsymbolmap[ricesymbol].value_counts()
            #sn = vsymbolmap['SN']
            startnum=nonenum['None']
        else:
            startnum = 0
        myquantsymbol='.'.join([myquantexchange,myquantsec])
        print myquantsymbol
        tofolder = "riceToMyquant %s.%s" % (myquantexchange, myquantsec)
        df=pd.DataFrame()
        df['trade_date']=vsymbolmap.loc[startnum:,'date']
        df['vsymbol']=myquantsymbol
        df['symbol']=vsymbolmap.loc[startnum:,ricesymbol]
        df.to_csv(tofolder+'\\'+myquantsymbol+'VirtualContract.csv')
        swapdf=findSwap(df)
        if swapdf is not None:
            swapdf.to_csv(tofolder+'\\'+myquantsymbol+'ContractSwap.csv')
    pass

def riceConcateToBar():
    contractlist = pd.read_excel(DC.PUBLIC_DATA_PATH + 'Contract.xlsx')['Contract']
    for symbol in contractlist:
        print symbol
        ricefolder="riceToMyquant %s\\"%(symbol)
        ricedata=pd.read_csv(ricefolder+"%sContractSwap.csv" % (symbol))
        bardata=DC.getContractSwaplist(symbol)
        bardata=bardata.loc[bardata['swaputc']>1462032000]
        bardata=pd.concat([ricedata,bardata])
        bardata.reset_index(drop=True,inplace=True)
        bardata = bardata.drop('Unnamed: 0', axis=1)
        bardata.to_csv('D:\\002 MakeLive\DataCollection\\vitualContract\\'+symbol+'ContractSwap.csv')
    pass

def copyRiceToQuant():
    # 将rice文件夹的合约切换文件拷到vitualContract文件夹
    import shutil
    destpath = 'D:\\002 MakeLive\DataCollection\\vitualContract\\'
    srcpath = 'D:\\002 MakeLive\DataCollection\\ricequant data'
    os.chdir(srcpath)
    symbollist = pd.read_excel('rice_symbol_map.xlsx')['symbol'].tolist()
    for symbol in symbollist:
        print symbol
        if symbol != 'INE.SC':
            srcfile = 'riceToMyquant '+symbol+'\\'+symbol+'ContractSwap.csv'
            dstfile = destpath+symbol+'ContractSwap.csv'
            shutil.copyfile(srcfile, dstfile)
    print "copy done"

#==================================================================================
def generateContractTable():
    # 生成主力合约列表总表
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    symbollist = pd.read_excel('rice_symbol_map.xlsx')
    symbolnum = symbollist.shape[0]
    contractdf = pd.DataFrame()
    for i in range(symbolnum):
        s = symbollist.iloc[i]
        #symbol = 'SHFE.RB'
        symbol = s['symbol']
        print symbol
        rice_main_id = s['order_book_id']
        rice_book_id = s['book_id']
        exchange = s['exchange']
        sec = s['sec']
        folder = 'riceToMyquant '+symbol
        vcdf = pd.read_csv(folder+'\\'+symbol+'VirtualContract.csv')
        grouped = vcdf.groupby('symbol')
        firstdate = grouped['trade_date'].first()
        tempdf = pd.DataFrame(firstdate)
        tempdf['lastdate'] = grouped['trade_date'].last()
        tempdf['symbol'] = symbol
        tempdf['rice_main_id'] = rice_main_id
        tempdf['rice_book_id'] = rice_book_id
        tempdf['exchange'] = exchange
        tempdf['sec'] = sec
        contractdf = pd.concat([contractdf, tempdf])

    contractdf.to_csv('contractTable.csv')

def generateContractTable2():
    # 为合约总表增加utc时间
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    symboldf = pd.read_csv('contractTable.csv')
    symbolnum = symboldf.shape[0]
    start_utc_list=[]
    end_utc_list=[]
    for i in range(symbolnum):
        s = symboldf.iloc[i]
        startdate = s['domain_start_date']
        enddate = s['domain_end_date']
        start_utc_list.append(int(time.mktime(time.strptime(startdate + ' 00:00:00', '%Y\%m\%d %H:%M:%S'))) - 10800)  # 简单写为3小时前是有问题的，应该写为上一交易日的21点！！！
        end_utc_list.append(int(time.mktime(time.strptime(enddate+' 16:00:00', '%Y\%m\%d %H:%M:%S'))))
    symboldf['domain_start_utc']=start_utc_list
    symboldf['domain_end_utc'] = end_utc_list

    symboldf.to_csv('contractTable2.csv',index=False)

if __name__ == '__main__':
    #filedf=pd.read_csv('D:\\002 MakeLive\DataCollection\\vitualContract\SHFE.RBVirtualContract.csv')
    #findSwap(filedf)
    #ricequantVsymbol()
    #os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    #riceConcateToBar()
    #enddate='2018-05-02'
    #updateContractSwap(enddate)
    #copyRiceToQuant()
    #===========================================
    generateContractTable2()
    pass