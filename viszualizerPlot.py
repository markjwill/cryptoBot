#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created by bu on 2018-01-17
"""
from __future__ import unicode_literals
import time
from datetime import datetime, timedelta
import ssl
import sys
import mariadb
import pprint
from pytz import timezone
import math
import plotext as plt

market = 'BTCUSDT'

startTime = 1614723828 #Tue Mar 02 2021 16:23:48 GMT-0600 (CST)
# endTime   = 1614725028 #Tue Mar 02 2021 16:43:48 GMT-0600 (CST)
endTime = 1614837600 #Thu Mar 04 2021 06:00:00 GMT+0000

date_ms = None
if len(sys.argv) > 1:
    date_ms = sys.argv[1]


class col:
    maxg = '\033[30m\033[42m'
    midg = '\033[32m\033[40m'
    ming = '\033[32m\033[47m'
    maxr = '\033[30m\033[41m'
    midr = '\033[31m\033[40m'
    minr = '\033[31m\033[47m'
    end = '\033[0m'


try:
    conn = mariadb.connect(
        user="MrBot",
        password="8fdvaoivposriong",
        host="127.0.0.1",
        port=3306,
        database="botscape"
    )
except mariadb.Error as e:
    print("Error connecting to MariaDB Platform: {0}".format(e))
    sys.exit(1)

# Get Cursor
cur = conn.cursor()

if date_ms is None:
    cur.execute("SELECT p_oneTwentyMin_changePercent,p_oneTwentyMin_volumePrMin,p_oneTwentyMin_tradesPrMin,p_oneTwentyMin_avgPrice,p_sixtyMin_changePercent,p_sixtyMin_volumePrMin,p_sixtyMin_tradesPrMin,p_sixtyMin_avgPrice,p_thirtyMin_changePercent,p_thirtyMin_volumePrMin,p_thirtyMin_tradesPrMin,p_thirtyMin_avgPrice,p_fifteenMin_changePercent,p_fifteenMin_volumePrMin,p_fifteenMin_tradesPrMin,p_fifteenMin_avgPrice,p_tenMin_changePercent,p_tenMin_volumePrMin,p_tenMin_tradesPrMin,p_tenMin_avgPrice,p_fiveMin_changePercent,p_fiveMin_volumePrMin,p_fiveMin_tradesPrMin,p_fiveMin_avgPrice,p_threeMin_changePercent,p_threeMin_volumePrMin,p_threeMin_tradesPrMin,p_threeMin_avgPrice,p_oneMin_changePercent,p_oneMin_volumePrMin,p_oneMin_tradesPrMin,p_oneMin_avgPrice,p_thirtySec_changePercent,p_thirtySec_volumePrMin,p_thirtySec_tradesPrMin,p_thirtySec_avgPrice,p_tenSec_changePercent,p_tenSec_volumePrMin,p_tenSec_tradesPrMin,p_tenSec_avgPrice,p_fiveSec_changePercent,p_fiveSec_volumePrMin,p_fiveSec_tradesPrMin,p_fiveSec_avgPrice,price,f_fiveSec_changePercent,f_tenSec_changePercent,f_thirtySec_changePercent,f_oneMin_changePercent,f_threeMin_changePercent,f_fiveMin_changePercent,f_tenMin_changePercent,f_fifteenMin_changePercent,f_thirtyMin_changePercent,f_sixtyMin_changePercent,f_oneTwentyMin_changePercent,f_fiveSec_lowPrice,f_tenSec_lowPrice,f_thirtySec_lowPrice,f_oneMin_lowPrice,f_threeMin_lowPrice,f_fiveMin_lowPrice,f_tenMin_lowPrice,f_fifteenMin_lowPrice,f_thirtyMin_lowPrice,f_sixtyMin_lowPrice,f_oneTwentyMin_lowPrice,f_fiveSec_highPrice,f_tenSec_highPrice,f_thirtySec_highPrice,f_oneMin_highPrice,f_threeMin_highPrice,f_fiveMin_highPrice,f_tenMin_highPrice,f_fifteenMin_highPrice,f_thirtyMin_highPrice,f_sixtyMin_highPrice,f_oneTwentyMin_highPrice FROM trades WHERE market = '{0:s}' AND p_oneTwentyMin_changePercent IS NOT NULL AND p_sixtyMin_changePercent IS NOT NULL AND p_thirtyMin_changePercent IS NOT NULL AND p_fifteenMin_changePercent IS NOT NULL AND p_tenMin_changePercent IS NOT NULL AND p_fiveMin_changePercent IS NOT NULL AND p_threeMin_changePercent IS NOT NULL AND p_oneMin_changePercent IS NOT NULL AND p_thirtySec_changePercent IS NOT NULL AND p_tenSec_changePercent IS NOT NULL AND p_fiveSec_changePercent IS NOT NULL AND price IS NOT NULL AND f_fiveSec_changePercent IS NOT NULL AND f_tenSec_changePercent IS NOT NULL AND f_thirtySec_changePercent IS NOT NULL AND f_oneMin_changePercent IS NOT NULL AND f_threeMin_changePercent IS NOT NULL AND f_fiveMin_changePercent IS NOT NULL AND f_tenMin_changePercent IS NOT NULL AND f_fifteenMin_changePercent IS NOT NULL AND f_thirtyMin_changePercent IS NOT NULL AND f_sixtyMin_changePercent IS NOT NULL AND f_oneTwentyMin_changePercent IS NOT NULL AND f_oneTwentyMin_lowPrice IS NOT NULL ORDER BY RAND() LIMIT 1".format(market))
else:
    cur.execute("SELECT p_oneTwentyMin_changePercent,p_oneTwentyMin_volumePrMin,p_oneTwentyMin_tradesPrMin,p_oneTwentyMin_avgPrice,p_sixtyMin_changePercent,p_sixtyMin_volumePrMin,p_sixtyMin_tradesPrMin,p_sixtyMin_avgPrice,p_thirtyMin_changePercent,p_thirtyMin_volumePrMin,p_thirtyMin_tradesPrMin,p_thirtyMin_avgPrice,p_fifteenMin_changePercent,p_fifteenMin_volumePrMin,p_fifteenMin_tradesPrMin,p_fifteenMin_avgPrice,p_tenMin_changePercent,p_tenMin_volumePrMin,p_tenMin_tradesPrMin,p_tenMin_avgPrice,p_fiveMin_changePercent,p_fiveMin_volumePrMin,p_fiveMin_tradesPrMin,p_fiveMin_avgPrice,p_threeMin_changePercent,p_threeMin_volumePrMin,p_threeMin_tradesPrMin,p_threeMin_avgPrice,p_oneMin_changePercent,p_oneMin_volumePrMin,p_oneMin_tradesPrMin,p_oneMin_avgPrice,p_thirtySec_changePercent,p_thirtySec_volumePrMin,p_thirtySec_tradesPrMin,p_thirtySec_avgPrice,p_tenSec_changePercent,p_tenSec_volumePrMin,p_tenSec_tradesPrMin,p_tenSec_avgPrice,p_fiveSec_changePercent,p_fiveSec_volumePrMin,p_fiveSec_tradesPrMin,p_fiveSec_avgPrice,price,f_fiveSec_changePercent,f_tenSec_changePercent,f_thirtySec_changePercent,f_oneMin_changePercent,f_threeMin_changePercent,f_fiveMin_changePercent,f_tenMin_changePercent,f_fifteenMin_changePercent,f_thirtyMin_changePercent,f_sixtyMin_changePercent,f_oneTwentyMin_changePercent,f_fiveSec_lowPrice,f_tenSec_lowPrice,f_thirtySec_lowPrice,f_oneMin_lowPrice,f_threeMin_lowPrice,f_fiveMin_lowPrice,f_tenMin_lowPrice,f_fifteenMin_lowPrice,f_thirtyMin_lowPrice,f_sixtyMin_lowPrice,f_oneTwentyMin_lowPrice,f_fiveSec_highPrice,f_tenSec_highPrice,f_thirtySec_highPrice,f_oneMin_highPrice,f_threeMin_highPrice,f_fiveMin_highPrice,f_tenMin_highPrice,f_fifteenMin_highPrice,f_thirtyMin_highPrice,f_sixtyMin_highPrice,f_oneTwentyMin_highPrice FROM trades WHERE market = '{0:s}' AND date_ms = {1:s} AND p_oneTwentyMin_changePercent IS NOT NULL AND p_sixtyMin_changePercent IS NOT NULL AND p_thirtyMin_changePercent IS NOT NULL AND p_fifteenMin_changePercent IS NOT NULL AND p_tenMin_changePercent IS NOT NULL AND p_fiveMin_changePercent IS NOT NULL AND p_threeMin_changePercent IS NOT NULL AND p_oneMin_changePercent IS NOT NULL AND p_thirtySec_changePercent IS NOT NULL AND p_tenSec_changePercent IS NOT NULL AND p_fiveSec_changePercent IS NOT NULL AND price IS NOT NULL AND f_fiveSec_changePercent IS NOT NULL AND f_tenSec_changePercent IS NOT NULL AND f_thirtySec_changePercent IS NOT NULL AND f_oneMin_changePercent IS NOT NULL AND f_threeMin_changePercent IS NOT NULL AND f_fiveMin_changePercent IS NOT NULL AND f_tenMin_changePercent IS NOT NULL AND f_fifteenMin_changePercent IS NOT NULL AND f_thirtyMin_changePercent IS NOT NULL AND f_sixtyMin_changePercent IS NOT NULL AND f_oneTwentyMin_changePercent IS NOT NULL AND f_oneTwentyMin_lowPrice IS NOT NULL ORDER BY RAND() LIMIT 1".format(market, date_ms))



trades = cur.fetchall()

def fixAverage(trade):
    return ((trade[3] - trade[44]) * 100 / trade[44]), ((trade[7] - trade[44]) * 100 / trade[44]), ((trade[11] - trade[44]) * 100 / trade[44]), ((trade[15] - trade[44]) * 100 / trade[44]), ((trade[19] - trade[44]) * 100 / trade[44]), ((trade[23] - trade[44]) * 100 / trade[44]), ((trade[27] - trade[44]) * 100 / trade[44]), ((trade[31] - trade[44]) * 100 / trade[44]), ((trade[35] - trade[44]) * 100 / trade[44]), ((trade[39] - trade[44]) * 100 / trade[44]), (( trade[43] - trade[44]) * 100 / trade[44])
def fixLow(trade):
    return ((trade[56] - trade[44]) * 100 / trade[44]), ((trade[57] - trade[44]) * 100 / trade[44]), ((trade[58] - trade[44]) * 100 / trade[44]), ((trade[59] - trade[44]) * 100 / trade[44]), ((trade[60] - trade[44]) * 100 / trade[44]), ((trade[61] - trade[44]) * 100 / trade[44]), ((trade[62] - trade[44]) * 100 / trade[44]), ((trade[63] - trade[44]) * 100 / trade[44]), ((trade[64] - trade[44]) * 100 / trade[44]), ((trade[65] - trade[44]) * 100 / trade[44]), (( trade[66] - trade[44]) * 100 / trade[44])
def fixHigh(trade):
    return ((trade[67] - trade[44]) * 100 / trade[44]), ((trade[68] - trade[44]) * 100 / trade[44]), ((trade[69] - trade[44]) * 100 / trade[44]), ((trade[70] - trade[44]) * 100 / trade[44]), ((trade[71] - trade[44]) * 100 / trade[44]), ((trade[72] - trade[44]) * 100 / trade[44]), ((trade[73] - trade[44]) * 100 / trade[44]), ((trade[74] - trade[44]) * 100 / trade[44]), ((trade[75] - trade[44]) * 100 / trade[44]), ((trade[76] - trade[44]) * 100 / trade[44]), (( trade[77] - trade[44]) * 100 / trade[44])

# 0   p_oneTwentyMin_changePercent
# 1   p_oneTwentyMin_volumePrMin
# 2   p_oneTwentyMin_tradesPrMin
# 3   p_oneTwentyMin_avgPrice
# 4   p_sixtyMin_changePercent
# 5   p_sixtyMin_volumePrMin
# 6   p_sixtyMin_tradesPrMin
# 7   p_sixtyMin_avgPrice
# 8   p_thirtyMin_changePercent
# 9   p_thirtyMin_volumePrMin
# 10  p_thirtyMin_tradesPrMin
# 11  p_thirtyMin_avgPrice
# 12  p_fifteenMin_changePercent
# 13  p_fifteenMin_volumePrMin
# 14  p_fifteenMin_tradesPrMin
# 15  p_fifteenMin_avgPrice
# 16  p_tenMin_changePercent
# 17  p_tenMin_volumePrMin
# 18  p_tenMin_tradesPrMin
# 19  p_tenMin_avgPrice
# 20  p_fiveMin_changePercent
# 21  p_fiveMin_volumePrMin
# 22  p_fiveMin_tradesPrMin
# 23  p_fiveMin_avgPrice
# 24  p_threeMin_changePercent
# 25  p_threeMin_volumePrMin
# 26  p_threeMin_tradesPrMin
# 27  p_threeMin_avgPrice
# 28  p_oneMin_changePercent
# 29  p_oneMin_volumePrMin
# 30  p_oneMin_tradesPrMin
# 31  p_oneMin_avgPrice
# 32  p_thirtySec_changePercent
# 33  p_thirtySec_volumePrMin
# 34  p_thirtySec_tradesPrMin
# 35  p_thirtySec_avgPrice
# 36  p_tenSec_changePercent
# 37  p_tenSec_volumePrMin
# 38  p_tenSec_tradesPrMin
# 39  p_tenSec_avgPrice
# 40  p_fiveSec_changePercent
# 41  p_fiveSec_volumePrMin
# 42  p_fiveSec_tradesPrMin
# 43  p_fiveSec_avgPrice
# 44  price
# 45  f_fiveSec_changePercent
# 46  f_tenSec_changePercent
# 47  f_thirtySec_changePercent
# 48  f_oneMin_changePercent
# 49  f_threeMin_changePercent
# 50  f_fiveMin_changePercent
# 51  f_tenMin_changePercent
# 52  f_fifteenMin_changePercent
# 53  f_thirtyMin_changePercent
# 54  f_sixtyMin_changePercent
# 55  f_oneTwentyMin_changePercent
# 56  f_fiveSec_lowPrice
# 57  f_tenSec_lowPrice
# 58  f_thirtySec_lowPrice
# 59  f_oneMin_lowPrice
# 60  f_threeMin_lowPrice
# 61  f_fiveMin_lowPrice
# 62  f_tenMin_lowPrice
# 63  f_fifteenMin_lowPrice
# 64  f_thirtyMin_lowPrice
# 65  f_sixtyMin_lowPrice
# 66  f_oneTwentyMin_lowPrice
# 67  f_fiveSec_highPrice
# 68  f_tenSec_highPrice
# 69  f_thirtySec_highPrice
# 70  f_oneMin_highPrice
# 71  f_threeMin_highPrice
# 72  f_fiveMin_highPrice
# 73  f_tenMin_highPrice
# 74  f_fifteenMin_highPrice
# 75  f_thirtyMin_highPrice
# 76  f_sixtyMin_highPrice
# 77  f_oneTwentyMin_highPrice

for trade in trades:
    avg = fixAverage(trade)
    low = fixLow(trade)
    high = fixHigh(trade)
    changePercent = [trade[0],trade[4],trade[8],trade[12],trade[16],trade[20],trade[24],trade[28],trade[32],trade[36],trade[40],trade[45],trade[46],trade[47],trade[48],trade[49],trade[50],trade[51],trade[52],trade[53],trade[54],trade[55]]

    avgPrice = [avg[0],avg[1],avg[2],avg[3],avg[4],avg[5],avg[6],avg[7],avg[8],avg[9],avg[10]]

    lowPrice = [0,0,0,0,0,0,0,0,0,0,0,low[0],low[1],low[2],low[3],low[4],low[5],low[6],low[7],low[8],low[9],low[10]]
    highPrice = [0,0,0,0,0,0,0,0,0,0,0,high[0],high[1],high[2],high[3],high[4],high[5],high[6],high[7],high[8],high[9],high[10]]

    volumnPrMin = [trade[1],trade[5],trade[9],trade[13],trade[17],trade[21],trade[25],trade[29],trade[33],trade[37],trade[41]]

    volumnPrMin = [trade[1],trade[5],trade[9],trade[13],trade[17],trade[21],trade[25],trade[29],trade[33],trade[37],trade[41]]

    tradesPrMin = [trade[2],trade[6],trade[10],trade[14],trade[18],trade[22],trade[26],trade[30],trade[34],trade[38],trade[42]]

plt.plot([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0], label="zero")
plt.scatter([-1], label="bottom")
plt.scatter([1], label="top")
plt.plot(volumnPrMin, label="volumnPrMin")
plt.plot(avgPrice, label="avgPrice")
plt.plot(lowPrice, label="lowPrice")
plt.plot(highPrice, label="highPrice")
plt.plot(changePercent, label="changePercent")
# plt.plot(tradesPrMin, label="tradesPrMin")
plt.show()
