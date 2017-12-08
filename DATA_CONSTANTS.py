# -*- coding: utf-8 -*-
#读取中文路径
PUBLIC_DATA_PATH=unicode('D:\\002 MakeLive\DataCollection\public data\\','utf-8')
RAW_DATA_PATH=unicode('D:\\002 MakeLive\DataCollection\\raw data\\','utf-8')
TICKS_DATA_PATH=unicode('D:\\002 MakeLive\DataCollection\\ticks data\\','utf-8')
BAR_DATA_PATH=unicode('D:\\002 MakeLive\DataCollection\\bar data\\','utf-8')

TICKS_DATA_START_DATE='2017-8-17'#包含了8-17日
LAST_CONCAT_DATA='2017-10-17'#记录上次汇总数据的时间，不包含当天（要再加上一天，要不然后面truncate会不对）