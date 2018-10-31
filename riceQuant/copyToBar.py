# -*- coding: utf-8 -*-
"""将更新后的数据拷贝到bar文件夹供正式使用"""
import time
import pandas as pd
import shutil
import os

def copyToBar(symbollist,domain_symbol, enddate, bar_type_list):
    srcpath = 'riceToMyquant '+domain_symbol
    destpath = 'D:\\002 MakeLive\DataCollection\\bar data\\'+domain_symbol+'\\'

    for symbol in symbollist:  # 拷贝单合约数据
        for bar_type in bar_type_list:
            print ("copying %s %d %s" % (symbol, bar_type, enddate))
            srcfile = "%s\\%s %d_%s.csv" % (srcpath, symbol, bar_type, enddate)
            dstfile = destpath + "%s %d.csv" % (symbol, bar_type)
            shutil.copyfile(srcfile, dstfile)



def copyToBar2(symbollist,domain_symbol,bar_type_list):
    """适配riceContractDataTransfer2"""
    #srcpath = 'riceToMyquant '+domain_symbol
    srcpath = ("rqdata_raw_%s\\" % domain_symbol)
    destpath = 'D:\\002 MakeLive\DataCollection\\bar data\\'+domain_symbol+'\\'

    for symbol in symbollist:  # 拷贝单合约数据
        for bar_type in bar_type_list:
            print ("copying %s %d" % (symbol, bar_type))
            srcfile = "%s\\%s %d.csv" % (srcpath, symbol, bar_type)
            dstfile = destpath + "%s %d.csv" % (symbol, bar_type)
            shutil.copyfile(srcfile, dstfile)
    """
    # 拷贝主连数据
    print ("copying %s %d %s" % (domain_symbol, 60, enddate))
    srcfile = "%s\\%s %d_%s.csv" % (srcpath, domain_symbol, 60, enddate)
    dstfile = destpath + "%s %d.csv" % (domain_symbol, 60)
    shutil.copyfile(srcfile, dstfile)
    print "copy done"
    """

def copyToBarTemp(symbollist,domain_symbol,bar_type_list):
    """适配riceContractDataTransfer2"""
    #srcpath = 'riceToMyquant '+domain_symbol
    srcpath = ("rqdata_raw_%s\\" % domain_symbol)
    destpath = 'D:\\002 MakeLive\DataCollection\\bar data_temp\\'+domain_symbol+'\\'
    try:
        os.mkdir(destpath)
    except:
        pass
    for symbol in symbollist:  # 拷贝单合约数据
        for bar_type in bar_type_list:
            print ("copying %s %d" % (symbol, bar_type))
            srcfile = "%s\\%s %d.csv" % (srcpath, symbol, bar_type)
            dstfile = destpath + "%s %d.csv" % (symbol, bar_type)
            shutil.copyfile(srcfile, dstfile)

def copyDominToBarTemp(domain_symbol,bar_type_list):
    """适配riceContractDataTransfer2"""
    #srcpath = 'riceToMyquant '+domain_symbol
    srcpath = 'D:\\002 MakeLive\DataCollection\\bar data\\'+domain_symbol+'\\'
    destpath = 'D:\\002 MakeLive\DataCollection\\bar data_temp\\'+domain_symbol+'\\'
    for bar_type in bar_type_list:
        print ("copying %s %d" % (domain_symbol, bar_type))
        srcfile = "%s\\%s %d.csv" % (srcpath, domain_symbol, bar_type)
        dstfile = destpath + "%s %d.csv" % (domain_symbol, bar_type)
        shutil.copyfile(srcfile, dstfile)

if __name__=='__main__':
    os.chdir('D:\\002 MakeLive\DataCollection\\ricequant data\\')
    bar_type_list = [60, 180, 300, 600, 900, 1800]
    #bar_type_list = [180]
    month_mode = False   # 为True表示是月度更新模式，只更新该月涉及的合约
    if month_mode:
        startdate = '2018-06-01'
        enddate = '2018-07-01'
        domain_map = pd.read_excel('D:\\002 MakeLive\DataCollection\public data\\domainMap.xlsx')
        contract_map = pd.read_csv('D:\\002 MakeLive\DataCollection\public data\\contractMap.csv')
        active_domain_list = domain_map.loc[domain_map['active'] == True]['symbol'].tolist()
        start_utc = int(time.mktime(time.strptime(startdate + ' 00:00:00', '%Y-%m-%d %H:%M:%S')))
        for domain_symbol in active_domain_list:
            symbol_contract_map = contract_map.loc[contract_map['domain_symbol'] == domain_symbol]
            modify_symbol = symbol_contract_map.loc[(symbol_contract_map['domain_start_utc'] > start_utc) | (symbol_contract_map['domain_end_utc'] > start_utc)]
            idlist = modify_symbol['symbol'].tolist()
            copyToBar(idlist, domain_symbol, enddate, bar_type_list)
    else:
        symboldf = pd.read_csv('D:\\002 MakeLive\DataCollection\public data\\contractMap.csv')
        domain = 'SHFE.RB'
        enddate = '2018-06-08'
        rbdf = symboldf.loc[symboldf['domain_symbol'] == domain]
        idlist = rbdf['symbol'].tolist()
        #idlist = ['RB1810']
        copyToBar(idlist, domain, enddate, bar_type_list)