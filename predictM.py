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

import categories

market = 'BTCUSDT'

date_ms = None
if len(sys.argv) > 1:
    date_ms = sys.argv[1]

startTime = 1614723828 #Tue Mar 02 2021 16:23:48 GMT-0600 (CST)
# endTime   = 1614725028 #Tue Mar 02 2021 16:43:48 GMT-0600 (CST)
endTime = 1614837600 #Thu Mar 04 2021 06:00:00 GMT+0000
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
    cur.execute("SELECT p_oneTwentyMin_changePercent,p_sixtyMin_changePercent,p_thirtyMin_changePercent,p_fifteenMin_changePercent,p_tenMin_changePercent,p_fiveMin_changePercent,p_threeMin_changePercent,p_oneMin_changePercent,p_thirtySec_changePercent,p_tenSec_changePercent,p_fiveSec_changePercent,price,f_fiveSec_changePercent,f_tenSec_changePercent,f_thirtySec_changePercent,f_oneMin_changePercent,f_threeMin_changePercent,f_fiveMin_changePercent,f_tenMin_changePercent,f_fifteenMin_changePercent,f_thirtyMin_changePercent,f_sixtyMin_changePercent,f_oneTwentyMin_changePercent, date_ms,f_fiveSec_lowPrice,f_tenSec_lowPrice,f_thirtySec_lowPrice,f_oneMin_lowPrice,f_threeMin_lowPrice,f_fiveMin_lowPrice,f_tenMin_lowPrice,f_fifteenMin_lowPrice,f_thirtyMin_lowPrice,f_sixtyMin_lowPrice,f_oneTwentyMin_lowPrice,f_fiveSec_highPrice,f_tenSec_highPrice,f_thirtySec_highPrice,f_oneMin_highPrice,f_threeMin_highPrice,f_fiveMin_highPrice,f_tenMin_highPrice,f_fifteenMin_highPrice,f_thirtyMin_highPrice,f_sixtyMin_highPrice,f_oneTwentyMin_highPrice,p_fiveMin_avgPrice,p_thirtyMin_avgPrice,p_fiveSec_avgPrice,p_oneTwentyMin_avgPrice, p_thirtySec_avgPrice FROM trades WHERE market = '{0:s}' AND p_oneTwentyMin_changePercent IS NOT NULL AND p_sixtyMin_changePercent IS NOT NULL AND p_thirtyMin_changePercent IS NOT NULL AND p_fifteenMin_changePercent IS NOT NULL AND p_tenMin_changePercent IS NOT NULL AND p_fiveMin_changePercent IS NOT NULL AND p_threeMin_changePercent IS NOT NULL AND p_oneMin_changePercent IS NOT NULL AND p_thirtySec_changePercent IS NOT NULL AND p_tenSec_changePercent IS NOT NULL AND p_fiveSec_changePercent IS NOT NULL AND price IS NOT NULL AND f_fiveSec_changePercent IS NOT NULL AND f_tenSec_changePercent IS NOT NULL AND f_thirtySec_changePercent IS NOT NULL AND f_oneMin_changePercent IS NOT NULL AND f_threeMin_changePercent IS NOT NULL AND f_fiveMin_changePercent IS NOT NULL AND f_tenMin_changePercent IS NOT NULL AND f_fifteenMin_changePercent IS NOT NULL AND f_thirtyMin_changePercent IS NOT NULL AND f_sixtyMin_changePercent IS NOT NULL AND f_oneTwentyMin_changePercent IS NOT NULL AND f_oneTwentyMin_lowPrice IS NOT NULL ORDER BY date_ms ASC".format(market, startTime, endTime))
else:
    cur.execute("SELECT p_oneTwentyMin_changePercent,p_sixtyMin_changePercent,p_thirtyMin_changePercent,p_fifteenMin_changePercent,p_tenMin_changePercent,p_fiveMin_changePercent,p_threeMin_changePercent,p_oneMin_changePercent,p_thirtySec_changePercent,p_tenSec_changePercent,p_fiveSec_changePercent,price,f_fiveSec_changePercent,f_tenSec_changePercent,f_thirtySec_changePercent,f_oneMin_changePercent,f_threeMin_changePercent,f_fiveMin_changePercent,f_tenMin_changePercent,f_fifteenMin_changePercent,f_thirtyMin_changePercent,f_sixtyMin_changePercent,f_oneTwentyMin_changePercent, date_ms,f_fiveSec_lowPrice,f_tenSec_lowPrice,f_thirtySec_lowPrice,f_oneMin_lowPrice,f_threeMin_lowPrice,f_fiveMin_lowPrice,f_tenMin_lowPrice,f_fifteenMin_lowPrice,f_thirtyMin_lowPrice,f_sixtyMin_lowPrice,f_oneTwentyMin_lowPrice,f_fiveSec_highPrice,f_tenSec_highPrice,f_thirtySec_highPrice,f_oneMin_highPrice,f_threeMin_highPrice,f_fiveMin_highPrice,f_tenMin_highPrice,f_fifteenMin_highPrice,f_thirtyMin_highPrice,f_sixtyMin_highPrice,f_oneTwentyMin_highPrice,p_fiveMin_avgPrice,p_thirtyMin_avgPrice,p_fiveSec_avgPrice,p_oneTwentyMin_avgPrice, p_thirtySec_avgPrice FROM trades WHERE market = '{0:s}' AND date_ms <= '{1:s}' AND p_oneTwentyMin_changePercent IS NOT NULL AND p_sixtyMin_changePercent IS NOT NULL AND p_thirtyMin_changePercent IS NOT NULL AND p_fifteenMin_changePercent IS NOT NULL AND p_tenMin_changePercent IS NOT NULL AND p_fiveMin_changePercent IS NOT NULL AND p_threeMin_changePercent IS NOT NULL AND p_oneMin_changePercent IS NOT NULL AND p_thirtySec_changePercent IS NOT NULL AND p_tenSec_changePercent IS NOT NULL AND p_fiveSec_changePercent IS NOT NULL AND price IS NOT NULL AND f_fiveSec_changePercent IS NOT NULL AND f_tenSec_changePercent IS NOT NULL AND f_thirtySec_changePercent IS NOT NULL AND f_oneMin_changePercent IS NOT NULL AND f_threeMin_changePercent IS NOT NULL AND f_fiveMin_changePercent IS NOT NULL AND f_tenMin_changePercent IS NOT NULL AND f_fifteenMin_changePercent IS NOT NULL AND f_thirtyMin_changePercent IS NOT NULL AND f_sixtyMin_changePercent IS NOT NULL AND f_oneTwentyMin_changePercent IS NOT NULL AND f_oneTwentyMin_lowPrice IS NOT NULL ORDER BY date_ms DESC LIMIT 2".format(market, date_ms))



trades = cur.fetchall()

if date_ms is not None:
    trades = trades[::-1]

def fixLowPrice(trade):
    return ((trade[24] - price) * 100 / price), ((trade[25] - price) * 100 / price), ((trade[26] - price) * 100 / price), ((trade[27] - price) * 100 / price), ((trade[28] - price) * 100 / price), ((trade[29] - price) * 100 / price), ((trade[30] - price) * 100 / price), ((trade[31] - price) * 100 / price), ((trade[32] - price) * 100 / price), ((trade[33] - price) * 100 / price), (( trade[34] - price) * 100 / price)

def fixHighPrice(trade):
    return ((trade[35] - price) * 100 / price), ((trade[36] - price) * 100 / price), ((trade[37] - price) * 100 / price), ((trade[38] - price) * 100 / price), ((trade[39] - price) * 100 / price), ((trade[40] - price) * 100 / price), ((trade[41] - price) * 100 / price), ((trade[42] - price) * 100 / price), ((trade[43] - price) * 100 / price), ((trade[44] - price) * 100 / price), (( trade[45] - price) * 100 / price)

def tradeWillSell(trade, high):
    if date_ms is not None:
        print("high "+str(high[2]))
        print("high "+str(high[3]))
        print("high "+str(high[4]))
        print("high "+str(high[5]))
        print("high "+str(high[6]))
        print("high "+str(high[7]))
        print("high "+str(high[8]))
        print("high "+str(high[9]))
        print("high "+str(high[10]))
    if (high[2] > sellPercent or
        high[3] > sellPercent or
        high[4] > sellPercent or
        high[5] > sellPercent or
        high[6] > sellPercent or
        high[7] > sellPercent or
        high[8] > sellPercent or
        high[9] > sellPercent or
        high[10] > sellPercent):
        return True
    else:
        return False

def tradeSellLikelyhood(trade, high):
    likelihood = 0;
    likelihood += (high[0] / sellPercent)
    likelihood += (high[1] / sellPercent)
    likelihood += (high[2] / sellPercent)
    likelihood += (high[3] / sellPercent)
    likelihood += (high[4] / sellPercent)
    likelihood += (high[5] / sellPercent)
    likelihood += (high[6] / sellPercent)
    likelihood += (high[7] / sellPercent)
    likelihood += (high[8] / sellPercent)
    likelihood += (high[9] / sellPercent)
    likelihood += (high[10] / sellPercent)
    return likelihood

def tradeBuyLikelyhood(trade, low):
    likelihood = 0;
    likelihood -= (low[0] / sellPercent)
    likelihood -= (low[1] / sellPercent)
    likelihood -= (low[2] / sellPercent)
    likelihood -= (low[3] / sellPercent)
    likelihood -= (low[4] / sellPercent)
    likelihood -= (low[5] / sellPercent)
    likelihood -= (low[6] / sellPercent)
    likelihood -= (low[7] / sellPercent)
    likelihood -= (low[8] / sellPercent)
    likelihood -= (low[9] / sellPercent)
    likelihood -= (low[10] / sellPercent)
    return likelihood

def tradeLikelyhood(trade, high, low):
    likelihood = 0;
    likelihood += (high[0] / sellPercent)
    likelihood += (high[1] / sellPercent)
    likelihood += (high[2] / sellPercent)
    likelihood += (high[3] / sellPercent)
    likelihood += (high[4] / sellPercent)
    likelihood += (high[5] / sellPercent)
    likelihood += (high[6] / sellPercent) # 10 min or less ^
    # likelihood += (high[7] / sellPercent)
    # likelihood += (high[8] / sellPercent)
    # likelihood += (high[9] / sellPercent)
    # likelihood += (high[10] / sellPercent)
    likelihood += (low[0] / sellPercent)
    likelihood += (low[1] / sellPercent)
    likelihood += (low[2] / sellPercent)
    likelihood += (low[3] / sellPercent)
    likelihood += (low[4] / sellPercent)
    likelihood += (low[5] / sellPercent)
    likelihood += (low[6] / sellPercent) # 10 min or less ^
    # likelihood += (low[7] / sellPercent)
    # likelihood += (low[8] / sellPercent)
    # likelihood += (low[9] / sellPercent)
    # likelihood += (low[10] / sellPercent)

    # Adjust extreme edge cases
    if likelihood > 9.99:
        return 9.99
    if likelihood < -9.99:
        return -9.99

    return likelihood

def tradeWillBuy(trade, low):
    if date_ms is not None:
        print("low "+str(low[2]))
        print("low "+str(low[3]))
        print("low "+str(low[4]))
        print("low "+str(low[5]))
        print("low "+str(low[6]))
        print("low "+str(low[7]))
        print("low "+str(low[8]))
        print("low "+str(low[9]))
        print("low "+str(low[10]))
    if (low[2] < buyPercent or
        low[3] < buyPercent or
        low[4] < buyPercent or
        low[5] < buyPercent or
        low[6] < buyPercent or
        low[7] < buyPercent or
        low[8] < buyPercent or
        low[9] < buyPercent or
        low[10] < buyPercent):
        return True
    else:
        return False

def tradeMin(trade):
    mini = 10
    if trade[0] < mini:
        mini = trade[0]
    if trade[1] < mini:
        mini = trade[1]
    if trade[2] < mini:
        mini = trade[2]
    if trade[3] < mini:
        mini = trade[3]
    if trade[4] < mini:
        mini = trade[4]
    if trade[5] < mini:
        mini = trade[5]
    if trade[6] < mini:
        mini = trade[6]
    if trade[7] < mini:
        mini = trade[7]
    if trade[8] < mini:
        mini = trade[8]
    if trade[9] < mini:
        mini = trade[9]
    if trade[10] < mini:
        mini = trade[10]
    return mini

def tradeMax(trade):
    mini = 10
    if trade[0] < mini:
        mini = trade[0]
    if trade[1] < mini:
        mini = trade[1]
    if trade[2] < mini:
        mini = trade[2]
    if trade[3] < mini:
        mini = trade[3]
    if trade[4] < mini:
        mini = trade[4]
    if trade[5] < mini:
        mini = trade[5]
    if trade[6] < mini:
        mini = trade[6]
    if trade[7] < mini:
        mini = trade[7]
    if trade[8] < mini:
        mini = trade[8]
    if trade[9] < mini:
        mini = trade[9]
    if trade[10] < mini:
        mini = trade[10]
    return mini

def tradeMax(trade):
    maxi = -10
    if trade[0] > maxi:
        maxi = trade[0]
    if trade[1] > maxi:
        maxi = trade[1]
    if trade[2] > maxi:
        maxi = trade[2]
    if trade[3] > maxi:
        maxi = trade[3]
    if trade[4] > maxi:
        maxi = trade[4]
    if trade[5] > maxi:
        maxi = trade[5]
    if trade[6] > maxi:
        maxi = trade[6]
    if trade[7] > maxi:
        maxi = trade[7]
    if trade[8] > maxi:
        maxi = trade[8]
    if trade[9] > maxi:
        maxi = trade[9]
    if trade[10] > maxi:
        maxi = trade[10]
    return maxi

def posCount(trade):
    count = 0
    if trade[0] > 0:
        count += 1
    if trade[1] > 0:
        count += 1
    if trade[2] > 0:
        count += 1
    if trade[3] > 0:
        count += 1
    if trade[4] > 0:
        count += 1
    if trade[5] > 0:
        count += 1
    if trade[6] > 0:
        count += 1
    if trade[7] > 0:
        count += 1
    if trade[8] > 0:
        count += 1
    if trade[9] > 0:
        count += 1
    if trade[10] > 0:
        count += 1
    return count

def lowMoveCount(trade):
    count = 0
    low = -0.5
    high = 0.5
    if trade[0] > low and trade[0] < high:
        count += 1
    if trade[1] > low and trade[1] < high:
        count += 1
    if trade[2] > low and trade[2] < high:
        count += 1
    if trade[3] > low and trade[3] < high:
        count += 1
    if trade[4] > low and trade[4] < high:
        count += 1
    if trade[5] > low and trade[5] < high:
        count += 1
    if trade[6] > low and trade[6] < high:
        count += 1
    if trade[7] > low and trade[7] < high:
        count += 1
    if trade[8] > low and trade[8] < high:
        count += 1
    if trade[9] > low and trade[9] < high:
        count += 1
    if trade[10] > low and trade[10] < high:
        count += 1
    return count

def highMoveCount(trade):
    count = 0
    low = -0.5
    high = 0.5
    if trade[0] < low or trade[0] > high:
        count += 1
    if trade[1] < low or trade[1] > high:
        count += 1
    if trade[2] < low or trade[2] > high:
        count += 1
    if trade[3] < low or trade[3] > high:
        count += 1
    if trade[4] < low or trade[4] > high:
        count += 1
    if trade[5] < low or trade[5] > high:
        count += 1
    if trade[6] < low or trade[6] > high:
        count += 1
    if trade[7] < low or trade[7] > high:
        count += 1
    if trade[8] < low or trade[8] > high:
        count += 1
    if trade[9] < low or trade[9] > high:
        count += 1
    if trade[10] < low or trade[10] > high:
        count += 1
    return count

def negCount(trade):
    return 11 - posCount(trade)

def posTrend(trade):
    if (trade[0] <= (trade[4] + 0.3) and
        trade[1] <= (trade[5] + 0.3) and
        trade[2] <= (trade[6] + 0.3) and
        trade[3] <= (trade[7] + 0.3) and
        trade[4] <= (trade[8] + 0.3) and
        trade[5] <= (trade[9] + 0.3) and
        trade[6] <= (trade[10] + 0.3)):
        return True
    else:
        return False

def negTrend(trade):
    if (trade[0] >= (trade[4] - 0.3) and
        trade[1] >= (trade[5] - 0.3) and
        trade[2] >= (trade[6] - 0.3) and
        trade[3] >= (trade[7] - 0.3) and
        trade[4] >= (trade[8] - 0.3) and
        trade[5] >= (trade[9] - 0.3) and
        trade[6] >= (trade[10] - 0.3)):
        return True
    else:
        return False

# print(int(datetime.now().timestamp()))
# 0   p_oneTwentyMin_changePercent
# 1   p_sixtyMin_changePercent
# 2   p_thirtyMin_changePercent
# 3   p_fifteenMin_changePercent
# 4   p_tenMin_changePercent
# 5   p_fiveMin_changePercent
# 6   p_threeMin_changePercent
# 7   p_oneMin_changePercent
# 8   p_thirtySec_changePercent
# 9   p_tenSec_changePercent
# 10  p_fiveSec_changePercent
# 11  price
# 12  f_fiveSec_changePercent
# 13  f_tenSec_changePercent
# 14  f_thirtySec_changePercent
# 15  f_oneMin_changePercent
# 16  f_threeMin_changePercent
# 17  f_fiveMin_changePercent
# 18  f_tenMin_changePercent
# 19  f_fifteenMin_changePercent
# 20  f_thirtyMin_changePercent
# 21  f_sixtyMin_changePercent
# 22  f_oneTwentyMin_changePercent
# 23  date_ms
# 24  f_fiveSec_lowPrice
# 25  f_tenSec_lowPrice
# 26  f_thirtySec_lowPrice
# 27  f_oneMin_lowPrice
# 28  f_threeMin_lowPrice
# 29  f_fiveMin_lowPrice
# 30  f_tenMin_lowPrice
# 31  f_fifteenMin_lowPrice
# 32  f_thirtyMin_lowPrice
# 33  f_sixtyMin_lowPrice
# 34  f_oneTwentyMin_lowPrice
# 35  f_fiveSec_highPrice
# 36  f_tenSec_highPrice
# 37  f_thirtySec_highPrice
# 38  f_oneMin_highPrice
# 39  f_threeMin_highPrice
# 40  f_fiveMin_highPrice
# 41  f_tenMin_highPrice
# 42  f_fifteenMin_highPrice
# 43  f_thirtyMin_highPrice
# 44  f_sixtyMin_highPrice
# 45  f_oneTwentyMin_highPrice
# 46  f_fiveMin_avgPrice
# 47  f_thirtyMin_avgPrice
# 48  p_fiveSec_avgPrice
# 49  p_oneTwentyMin_avgPrice

correct = 0
incorrect = 0
unknown = 0
total = 0

sellPercent=0.32
buyPercent=-0.32

previousFiveAvg = None

scores = {
    1: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    2: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    3: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    4: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    5: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    6: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    7: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    8: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    9: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    10: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    11: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    12: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    13: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    14: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    15: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    16: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    17: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    18: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    19: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
    20: {
        'correct': 0,
        'incorrect': 0,
        'samples': [],
        'likelyCorrect': [],
        'likelyIncorrect': []
    },
}

def scoreBuySlope(d):
    if d['fiveSlope'] < -0.4:
        return d['fiveSlope'] * -14.80048
    else:
        return 1.0

testNumber = 1
first = True
X = []
###
# PFTO-FLTFOH
# Means:
# Price is above the 5min avg
# 5min avg is above the 30min avg
# 30min avg is above the 120 min avg
# 5min avg is trending low ( down )
# 30min avg is trending flat-ish
# 120min avg is tredngin high ( up )
#
###
# M = {
#     'PFTO-LLL' : [],
#     'PFTO-FLL' : [],
#     'PFTO-HLL' : [],
#     'PFTO-LFL' : [],
#     'PFTO-FFL' : [],
#     'PFTO-HFL' : [],
#     'PFTO-LHL' : [],
#     'PFTO-FHL' : [],
#     'PFTO-HHL' : [],
#     'PFTO-LLF' : [],
#     'PFTO-FLF' : [],
#     'PFTO-HLF' : [],
#     'PFTO-LFF' : [],
#     'PFTO-FFF' : [],
#     'PFTO-HFF' : [],
#     'PFTO-LHF' : [],
#     'PFTO-FHF' : [],
#     'PFTO-HHF' : [],
#     'PFTO-LLH' : [],
#     'PFTO-FLH' : [],
#     'PFTO-HLH' : [],
#     'PFTO-LFH' : [],
#     'PFTO-FFH' : [],
#     'PFTO-HFH' : [],
#     'PFTO-LHH' : [],
#     'PFTO-FHH' : [],
#     'PFTO-HHH' : [],
#     'PFOT-LLL' : [],
#     'PFOT-FLL' : [],
#     'PFOT-HLL' : [],
#     'PFOT-LFL' : [],
#     'PFOT-FFL' : [],
#     'PFOT-HFL' : [],
#     'PFOT-LHL' : [],
#     'PFOT-FHL' : [],
#     'PFOT-HHL' : [],
#     'PFOT-LLF' : [],
#     'PFOT-FLF' : [],
#     'PFOT-HLF' : [],
#     'PFOT-LFF' : [],
#     'PFOT-FFF' : [],
#     'PFOT-HFF' : [],
#     'PFOT-LHF' : [],
#     'PFOT-FHF' : [],
#     'PFOT-HHF' : [],
#     'PFOT-LLH' : [],
#     'PFOT-FLH' : [],
#     'PFOT-HLH' : [],
#     'PFOT-LFH' : [],
#     'PFOT-FFH' : [],
#     'PFOT-HFH' : [],
#     'PFOT-LHH' : [],
#     'PFOT-FHH' : [],
#     'PFOT-HHH' : [],
#     'PTFO-LLL' : [],
#     'PTFO-FLL' : [],
#     'PTFO-HLL' : [],
#     'PTFO-LFL' : [],
#     'PTFO-FFL' : [],
#     'PTFO-HFL' : [],
#     'PTFO-LHL' : [],
#     'PTFO-FHL' : [],
#     'PTFO-HHL' : [],
#     'PTFO-LLF' : [],
#     'PTFO-FLF' : [],
#     'PTFO-HLF' : [],
#     'PTFO-LFF' : [],
#     'PTFO-FFF' : [],
#     'PTFO-HFF' : [],
#     'PTFO-LHF' : [],
#     'PTFO-FHF' : [],
#     'PTFO-HHF' : [],
#     'PTFO-LLH' : [],
#     'PTFO-FLH' : [],
#     'PTFO-HLH' : [],
#     'PTFO-LFH' : [],
#     'PTFO-FFH' : [],
#     'PTFO-HFH' : [],
#     'PTFO-LHH' : [],
#     'PTFO-FHH' : [],
#     'PTFO-HHH' : [],
#     'PTOF-LLL' : [],
#     'PTOF-FLL' : [],
#     'PTOF-HLL' : [],
#     'PTOF-LFL' : [],
#     'PTOF-FFL' : [],
#     'PTOF-HFL' : [],
#     'PTOF-LHL' : [],
#     'PTOF-FHL' : [],
#     'PTOF-HHL' : [],
#     'PTOF-LLF' : [],
#     'PTOF-FLF' : [],
#     'PTOF-HLF' : [],
#     'PTOF-LFF' : [],
#     'PTOF-FFF' : [],
#     'PTOF-HFF' : [],
#     'PTOF-LHF' : [],
#     'PTOF-FHF' : [],
#     'PTOF-HHF' : [],
#     'PTOF-LLH' : [],
#     'PTOF-FLH' : [],
#     'PTOF-HLH' : [],
#     'PTOF-LFH' : [],
#     'PTOF-FFH' : [],
#     'PTOF-HFH' : [],
#     'PTOF-LHH' : [],
#     'PTOF-FHH' : [],
#     'PTOF-HHH' : [],
#     'POFT-LLL' : [],
#     'POFT-FLL' : [],
#     'POFT-HLL' : [],
#     'POFT-LFL' : [],
#     'POFT-FFL' : [],
#     'POFT-HFL' : [],
#     'POFT-LHL' : [],
#     'POFT-FHL' : [],
#     'POFT-HHL' : [],
#     'POFT-LLF' : [],
#     'POFT-FLF' : [],
#     'POFT-HLF' : [],
#     'POFT-LFF' : [],
#     'POFT-FFF' : [],
#     'POFT-HFF' : [],
#     'POFT-LHF' : [],
#     'POFT-FHF' : [],
#     'POFT-HHF' : [],
#     'POFT-LLH' : [],
#     'POFT-FLH' : [],
#     'POFT-HLH' : [],
#     'POFT-LFH' : [],
#     'POFT-FFH' : [],
#     'POFT-HFH' : [],
#     'POFT-LHH' : [],
#     'POFT-FHH' : [],
#     'POFT-HHH' : [],
#     'POTF-LLL' : [],
#     'POTF-FLL' : [],
#     'POTF-HLL' : [],
#     'POTF-LFL' : [],
#     'POTF-FFL' : [],
#     'POTF-HFL' : [],
#     'POTF-LHL' : [],
#     'POTF-FHL' : [],
#     'POTF-HHL' : [],
#     'POTF-LLF' : [],
#     'POTF-FLF' : [],
#     'POTF-HLF' : [],
#     'POTF-LFF' : [],
#     'POTF-FFF' : [],
#     'POTF-HFF' : [],
#     'POTF-LHF' : [],
#     'POTF-FHF' : [],
#     'POTF-HHF' : [],
#     'POTF-LLH' : [],
#     'POTF-FLH' : [],
#     'POTF-HLH' : [],
#     'POTF-LFH' : [],
#     'POTF-FFH' : [],
#     'POTF-HFH' : [],
#     'POTF-LHH' : [],
#     'POTF-FHH' : [],
#     'POTF-HHH' : [],
#     'FPTO-LLL' : [],
#     'FPTO-FLL' : [],
#     'FPTO-HLL' : [],
#     'FPTO-LFL' : [],
#     'FPTO-FFL' : [],
#     'FPTO-HFL' : [],
#     'FPTO-LHL' : [],
#     'FPTO-FHL' : [],
#     'FPTO-HHL' : [],
#     'FPTO-LLF' : [],
#     'FPTO-FLF' : [],
#     'FPTO-HLF' : [],
#     'FPTO-LFF' : [],
#     'FPTO-FFF' : [],
#     'FPTO-HFF' : [],
#     'FPTO-LHF' : [],
#     'FPTO-FHF' : [],
#     'FPTO-HHF' : [],
#     'FPTO-LLH' : [],
#     'FPTO-FLH' : [],
#     'FPTO-HLH' : [],
#     'FPTO-LFH' : [],
#     'FPTO-FFH' : [],
#     'FPTO-HFH' : [],
#     'FPTO-LHH' : [],
#     'FPTO-FHH' : [],
#     'FPTO-HHH' : [],
#     'FPOT-LLL' : [],
#     'FPOT-FLL' : [],
#     'FPOT-HLL' : [],
#     'FPOT-LFL' : [],
#     'FPOT-FFL' : [],
#     'FPOT-HFL' : [],
#     'FPOT-LHL' : [],
#     'FPOT-FHL' : [],
#     'FPOT-HHL' : [],
#     'FPOT-LLF' : [],
#     'FPOT-FLF' : [],
#     'FPOT-HLF' : [],
#     'FPOT-LFF' : [],
#     'FPOT-FFF' : [],
#     'FPOT-HFF' : [],
#     'FPOT-LHF' : [],
#     'FPOT-FHF' : [],
#     'FPOT-HHF' : [],
#     'FPOT-LLH' : [],
#     'FPOT-FLH' : [],
#     'FPOT-HLH' : [],
#     'FPOT-LFH' : [],
#     'FPOT-FFH' : [],
#     'FPOT-HFH' : [],
#     'FPOT-LHH' : [],
#     'FPOT-FHH' : [],
#     'FPOT-HHH' : [],
#     'FTPO-LLL' : [],
#     'FTPO-FLL' : [],
#     'FTPO-HLL' : [],
#     'FTPO-LFL' : [],
#     'FTPO-FFL' : [],
#     'FTPO-HFL' : [],
#     'FTPO-LHL' : [],
#     'FTPO-FHL' : [],
#     'FTPO-HHL' : [],
#     'FTPO-LLF' : [],
#     'FTPO-FLF' : [],
#     'FTPO-HLF' : [],
#     'FTPO-LFF' : [],
#     'FTPO-FFF' : [],
#     'FTPO-HFF' : [],
#     'FTPO-LHF' : [],
#     'FTPO-FHF' : [],
#     'FTPO-HHF' : [],
#     'FTPO-LLH' : [],
#     'FTPO-FLH' : [],
#     'FTPO-HLH' : [],
#     'FTPO-LFH' : [],
#     'FTPO-FFH' : [],
#     'FTPO-HFH' : [],
#     'FTPO-LHH' : [],
#     'FTPO-FHH' : [],
#     'FTPO-HHH' : [],
#     'FTOP-LLL' : [],
#     'FTOP-FLL' : [],
#     'FTOP-HLL' : [],
#     'FTOP-LFL' : [],
#     'FTOP-FFL' : [],
#     'FTOP-HFL' : [],
#     'FTOP-LHL' : [],
#     'FTOP-FHL' : [],
#     'FTOP-HHL' : [],
#     'FTOP-LLF' : [],
#     'FTOP-FLF' : [],
#     'FTOP-HLF' : [],
#     'FTOP-LFF' : [],
#     'FTOP-FFF' : [],
#     'FTOP-HFF' : [],
#     'FTOP-LHF' : [],
#     'FTOP-FHF' : [],
#     'FTOP-HHF' : [],
#     'FTOP-LLH' : [],
#     'FTOP-FLH' : [],
#     'FTOP-HLH' : [],
#     'FTOP-LFH' : [],
#     'FTOP-FFH' : [],
#     'FTOP-HFH' : [],
#     'FTOP-LHH' : [],
#     'FTOP-FHH' : [],
#     'FTOP-HHH' : [],
#     'FOPT-LLL' : [],
#     'FOPT-FLL' : [],
#     'FOPT-HLL' : [],
#     'FOPT-LFL' : [],
#     'FOPT-FFL' : [],
#     'FOPT-HFL' : [],
#     'FOPT-LHL' : [],
#     'FOPT-FHL' : [],
#     'FOPT-HHL' : [],
#     'FOPT-LLF' : [],
#     'FOPT-FLF' : [],
#     'FOPT-HLF' : [],
#     'FOPT-LFF' : [],
#     'FOPT-FFF' : [],
#     'FOPT-HFF' : [],
#     'FOPT-LHF' : [],
#     'FOPT-FHF' : [],
#     'FOPT-HHF' : [],
#     'FOPT-LLH' : [],
#     'FOPT-FLH' : [],
#     'FOPT-HLH' : [],
#     'FOPT-LFH' : [],
#     'FOPT-FFH' : [],
#     'FOPT-HFH' : [],
#     'FOPT-LHH' : [],
#     'FOPT-FHH' : [],
#     'FOPT-HHH' : [],
#     'FOTP-LLL' : [],
#     'FOTP-FLL' : [],
#     'FOTP-HLL' : [],
#     'FOTP-LFL' : [],
#     'FOTP-FFL' : [],
#     'FOTP-HFL' : [],
#     'FOTP-LHL' : [],
#     'FOTP-FHL' : [],
#     'FOTP-HHL' : [],
#     'FOTP-LLF' : [],
#     'FOTP-FLF' : [],
#     'FOTP-HLF' : [],
#     'FOTP-LFF' : [],
#     'FOTP-FFF' : [],
#     'FOTP-HFF' : [],
#     'FOTP-LHF' : [],
#     'FOTP-FHF' : [],
#     'FOTP-HHF' : [],
#     'FOTP-LLH' : [],
#     'FOTP-FLH' : [],
#     'FOTP-HLH' : [],
#     'FOTP-LFH' : [],
#     'FOTP-FFH' : [],
#     'FOTP-HFH' : [],
#     'FOTP-LHH' : [],
#     'FOTP-FHH' : [],
#     'FOTP-HHH' : [],
#     'TPFO-LLL' : [],
#     'TPFO-FLL' : [],
#     'TPFO-HLL' : [],
#     'TPFO-LFL' : [],
#     'TPFO-FFL' : [],
#     'TPFO-HFL' : [],
#     'TPFO-LHL' : [],
#     'TPFO-FHL' : [],
#     'TPFO-HHL' : [],
#     'TPFO-LLF' : [],
#     'TPFO-FLF' : [],
#     'TPFO-HLF' : [],
#     'TPFO-LFF' : [],
#     'TPFO-FFF' : [],
#     'TPFO-HFF' : [],
#     'TPFO-LHF' : [],
#     'TPFO-FHF' : [],
#     'TPFO-HHF' : [],
#     'TPFO-LLH' : [],
#     'TPFO-FLH' : [],
#     'TPFO-HLH' : [],
#     'TPFO-LFH' : [],
#     'TPFO-FFH' : [],
#     'TPFO-HFH' : [],
#     'TPFO-LHH' : [],
#     'TPFO-FHH' : [],
#     'TPFO-HHH' : [],
#     'TPOF-LLL' : [],
#     'TPOF-FLL' : [],
#     'TPOF-HLL' : [],
#     'TPOF-LFL' : [],
#     'TPOF-FFL' : [],
#     'TPOF-HFL' : [],
#     'TPOF-LHL' : [],
#     'TPOF-FHL' : [],
#     'TPOF-HHL' : [],
#     'TPOF-LLF' : [],
#     'TPOF-FLF' : [],
#     'TPOF-HLF' : [],
#     'TPOF-LFF' : [],
#     'TPOF-FFF' : [],
#     'TPOF-HFF' : [],
#     'TPOF-LHF' : [],
#     'TPOF-FHF' : [],
#     'TPOF-HHF' : [],
#     'TPOF-LLH' : [],
#     'TPOF-FLH' : [],
#     'TPOF-HLH' : [],
#     'TPOF-LFH' : [],
#     'TPOF-FFH' : [],
#     'TPOF-HFH' : [],
#     'TPOF-LHH' : [],
#     'TPOF-FHH' : [],
#     'TPOF-HHH' : [],
#     'TFPO-LLL' : [],
#     'TFPO-FLL' : [],
#     'TFPO-HLL' : [],
#     'TFPO-LFL' : [],
#     'TFPO-FFL' : [],
#     'TFPO-HFL' : [],
#     'TFPO-LHL' : [],
#     'TFPO-FHL' : [],
#     'TFPO-HHL' : [],
#     'TFPO-LLF' : [],
#     'TFPO-FLF' : [],
#     'TFPO-HLF' : [],
#     'TFPO-LFF' : [],
#     'TFPO-FFF' : [],
#     'TFPO-HFF' : [],
#     'TFPO-LHF' : [],
#     'TFPO-FHF' : [],
#     'TFPO-HHF' : [],
#     'TFPO-LLH' : [],
#     'TFPO-FLH' : [],
#     'TFPO-HLH' : [],
#     'TFPO-LFH' : [],
#     'TFPO-FFH' : [],
#     'TFPO-HFH' : [],
#     'TFPO-LHH' : [],
#     'TFPO-FHH' : [],
#     'TFPO-HHH' : [],
#     'TFOP-LLL' : [],
#     'TFOP-FLL' : [],
#     'TFOP-HLL' : [],
#     'TFOP-LFL' : [],
#     'TFOP-FFL' : [],
#     'TFOP-HFL' : [],
#     'TFOP-LHL' : [],
#     'TFOP-FHL' : [],
#     'TFOP-HHL' : [],
#     'TFOP-LLF' : [],
#     'TFOP-FLF' : [],
#     'TFOP-HLF' : [],
#     'TFOP-LFF' : [],
#     'TFOP-FFF' : [],
#     'TFOP-HFF' : [],
#     'TFOP-LHF' : [],
#     'TFOP-FHF' : [],
#     'TFOP-HHF' : [],
#     'TFOP-LLH' : [],
#     'TFOP-FLH' : [],
#     'TFOP-HLH' : [],
#     'TFOP-LFH' : [],
#     'TFOP-FFH' : [],
#     'TFOP-HFH' : [],
#     'TFOP-LHH' : [],
#     'TFOP-FHH' : [],
#     'TFOP-HHH' : [],
#     'TOPF-LLL' : [],
#     'TOPF-FLL' : [],
#     'TOPF-HLL' : [],
#     'TOPF-LFL' : [],
#     'TOPF-FFL' : [],
#     'TOPF-HFL' : [],
#     'TOPF-LHL' : [],
#     'TOPF-FHL' : [],
#     'TOPF-HHL' : [],
#     'TOPF-LLF' : [],
#     'TOPF-FLF' : [],
#     'TOPF-HLF' : [],
#     'TOPF-LFF' : [],
#     'TOPF-FFF' : [],
#     'TOPF-HFF' : [],
#     'TOPF-LHF' : [],
#     'TOPF-FHF' : [],
#     'TOPF-HHF' : [],
#     'TOPF-LLH' : [],
#     'TOPF-FLH' : [],
#     'TOPF-HLH' : [],
#     'TOPF-LFH' : [],
#     'TOPF-FFH' : [],
#     'TOPF-HFH' : [],
#     'TOPF-LHH' : [],
#     'TOPF-FHH' : [],
#     'TOPF-HHH' : [],
#     'TOFP-LLL' : [],
#     'TOFP-FLL' : [],
#     'TOFP-HLL' : [],
#     'TOFP-LFL' : [],
#     'TOFP-FFL' : [],
#     'TOFP-HFL' : [],
#     'TOFP-LHL' : [],
#     'TOFP-FHL' : [],
#     'TOFP-HHL' : [],
#     'TOFP-LLF' : [],
#     'TOFP-FLF' : [],
#     'TOFP-HLF' : [],
#     'TOFP-LFF' : [],
#     'TOFP-FFF' : [],
#     'TOFP-HFF' : [],
#     'TOFP-LHF' : [],
#     'TOFP-FHF' : [],
#     'TOFP-HHF' : [],
#     'TOFP-LLH' : [],
#     'TOFP-FLH' : [],
#     'TOFP-HLH' : [],
#     'TOFP-LFH' : [],
#     'TOFP-FFH' : [],
#     'TOFP-HFH' : [],
#     'TOFP-LHH' : [],
#     'TOFP-FHH' : [],
#     'TOFP-HHH' : [],
#     'OPFT-LLL' : [],
#     'OPFT-FLL' : [],
#     'OPFT-HLL' : [],
#     'OPFT-LFL' : [],
#     'OPFT-FFL' : [],
#     'OPFT-HFL' : [],
#     'OPFT-LHL' : [],
#     'OPFT-FHL' : [],
#     'OPFT-HHL' : [],
#     'OPFT-LLF' : [],
#     'OPFT-FLF' : [],
#     'OPFT-HLF' : [],
#     'OPFT-LFF' : [],
#     'OPFT-FFF' : [],
#     'OPFT-HFF' : [],
#     'OPFT-LHF' : [],
#     'OPFT-FHF' : [],
#     'OPFT-HHF' : [],
#     'OPFT-LLH' : [],
#     'OPFT-FLH' : [],
#     'OPFT-HLH' : [],
#     'OPFT-LFH' : [],
#     'OPFT-FFH' : [],
#     'OPFT-HFH' : [],
#     'OPFT-LHH' : [],
#     'OPFT-FHH' : [],
#     'OPFT-HHH' : [],
#     'OPTF-LLL' : [],
#     'OPTF-FLL' : [],
#     'OPTF-HLL' : [],
#     'OPTF-LFL' : [],
#     'OPTF-FFL' : [],
#     'OPTF-HFL' : [],
#     'OPTF-LHL' : [],
#     'OPTF-FHL' : [],
#     'OPTF-HHL' : [],
#     'OPTF-LLF' : [],
#     'OPTF-FLF' : [],
#     'OPTF-HLF' : [],
#     'OPTF-LFF' : [],
#     'OPTF-FFF' : [],
#     'OPTF-HFF' : [],
#     'OPTF-LHF' : [],
#     'OPTF-FHF' : [],
#     'OPTF-HHF' : [],
#     'OPTF-LLH' : [],
#     'OPTF-FLH' : [],
#     'OPTF-HLH' : [],
#     'OPTF-LFH' : [],
#     'OPTF-FFH' : [],
#     'OPTF-HFH' : [],
#     'OPTF-LHH' : [],
#     'OPTF-FHH' : [],
#     'OPTF-HHH' : [],
#     'OFPT-LLL' : [],
#     'OFPT-FLL' : [],
#     'OFPT-HLL' : [],
#     'OFPT-LFL' : [],
#     'OFPT-FFL' : [],
#     'OFPT-HFL' : [],
#     'OFPT-LHL' : [],
#     'OFPT-FHL' : [],
#     'OFPT-HHL' : [],
#     'OFPT-LLF' : [],
#     'OFPT-FLF' : [],
#     'OFPT-HLF' : [],
#     'OFPT-LFF' : [],
#     'OFPT-FFF' : [],
#     'OFPT-HFF' : [],
#     'OFPT-LHF' : [],
#     'OFPT-FHF' : [],
#     'OFPT-HHF' : [],
#     'OFPT-LLH' : [],
#     'OFPT-FLH' : [],
#     'OFPT-HLH' : [],
#     'OFPT-LFH' : [],
#     'OFPT-FFH' : [],
#     'OFPT-HFH' : [],
#     'OFPT-LHH' : [],
#     'OFPT-FHH' : [],
#     'OFPT-HHH' : [],
#     'OFTP-LLL' : [],
#     'OFTP-FLL' : [],
#     'OFTP-HLL' : [],
#     'OFTP-LFL' : [],
#     'OFTP-FFL' : [],
#     'OFTP-HFL' : [],
#     'OFTP-LHL' : [],
#     'OFTP-FHL' : [],
#     'OFTP-HHL' : [],
#     'OFTP-LLF' : [],
#     'OFTP-FLF' : [],
#     'OFTP-HLF' : [],
#     'OFTP-LFF' : [],
#     'OFTP-FFF' : [],
#     'OFTP-HFF' : [],
#     'OFTP-LHF' : [],
#     'OFTP-FHF' : [],
#     'OFTP-HHF' : [],
#     'OFTP-LLH' : [],
#     'OFTP-FLH' : [],
#     'OFTP-HLH' : [],
#     'OFTP-LFH' : [],
#     'OFTP-FFH' : [],
#     'OFTP-HFH' : [],
#     'OFTP-LHH' : [],
#     'OFTP-FHH' : [],
#     'OFTP-HHH' : [],
#     'OTPF-LLL' : [],
#     'OTPF-FLL' : [],
#     'OTPF-HLL' : [],
#     'OTPF-LFL' : [],
#     'OTPF-FFL' : [],
#     'OTPF-HFL' : [],
#     'OTPF-LHL' : [],
#     'OTPF-FHL' : [],
#     'OTPF-HHL' : [],
#     'OTPF-LLF' : [],
#     'OTPF-FLF' : [],
#     'OTPF-HLF' : [],
#     'OTPF-LFF' : [],
#     'OTPF-FFF' : [],
#     'OTPF-HFF' : [],
#     'OTPF-LHF' : [],
#     'OTPF-FHF' : [],
#     'OTPF-HHF' : [],
#     'OTPF-LLH' : [],
#     'OTPF-FLH' : [],
#     'OTPF-HLH' : [],
#     'OTPF-LFH' : [],
#     'OTPF-FFH' : [],
#     'OTPF-HFH' : [],
#     'OTPF-LHH' : [],
#     'OTPF-FHH' : [],
#     'OTPF-HHH' : [],
#     'OTFP-LLL' : [],
#     'OTFP-FLL' : [],
#     'OTFP-HLL' : [],
#     'OTFP-LFL' : [],
#     'OTFP-FFL' : [],
#     'OTFP-HFL' : [],
#     'OTFP-LHL' : [],
#     'OTFP-FHL' : [],
#     'OTFP-HHL' : [],
#     'OTFP-LLF' : [],
#     'OTFP-FLF' : [],
#     'OTFP-HLF' : [],
#     'OTFP-LFF' : [],
#     'OTFP-FFF' : [],
#     'OTFP-HFF' : [],
#     'OTFP-LHF' : [],
#     'OTFP-FHF' : [],
#     'OTFP-HHF' : [],
#     'OTFP-LLH' : [],
#     'OTFP-FLH' : [],
#     'OTFP-HLH' : [],
#     'OTFP-LFH' : [],
#     'OTFP-FFH' : [],
#     'OTFP-HFH' : [],
#     'OTFP-LHH' : [],
#     'OTFP-FHH' : [],
#     'OTFP-HHH' : [],
#     'WRONG   ' : []
# }

# def getSlopeString(fiveSlope, thirtySlope, oneTwentySlope):
#     string = ''
#     if fiveSlope <= slopeLowCut:
#         string += 'L'
#     elif fiveSlope <= slopeHighCut:
#         string += 'F'
#     else:
#         string += 'H'

#     if thirtySlope <= slopeLowCut:
#         string += 'L'
#     elif thirtySlope <= slopeHighCut:
#         string += 'F'
#     else:
#         string += 'H'

#     if oneTwentySlope <= slopeLowCut:
#         string += 'L'
#     elif oneTwentySlope <= slopeHighCut:
#         string += 'F'
#     else:
#         string += 'H'

#     return string

# def getRollingString(price, fiveAvg, thirtyAvg, oneTwentyAvg):
#     if price >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg:
#         return 'PFTO'
#     if price >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg:
#         return 'PFOT'
#     if price >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= oneTwentyAvg:
#         return 'PTFO'
#     if price >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg:
#         return 'PTOF'
#     if price >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= thirtyAvg:
#         return 'POFT'
#     if price >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= fiveAvg:
#         return 'POTF'

#     if fiveAvg >= price and price >= thirtyAvg and thirtyAvg >= oneTwentyAvg:
#         return 'FPTO'
#     if fiveAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg:
#         return 'FPOT'
#     if fiveAvg >= thirtyAvg and thirtyAvg >= price and price >= oneTwentyAvg:
#         return 'FTPO'
#     if fiveAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= price:
#         return 'FTOP'
#     if fiveAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= thirtyAvg:
#         return 'FOPT'
#     if fiveAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= price:
#         return 'FOTP'

#     if thirtyAvg >= price and price >= fiveAvg and fiveAvg >= oneTwentyAvg:
#         return 'TPFO'
#     if thirtyAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= fiveAvg:
#         return 'TPOF'
#     if thirtyAvg >= fiveAvg and fiveAvg >= price and price >= oneTwentyAvg:
#         return 'TFPO'
#     if thirtyAvg >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= price:
#         return 'TFOP'
#     if thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= fiveAvg:
#         return 'TOPF'
#     if thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= price:
#         return 'TOFP'

#     if oneTwentyAvg >= price and price >= fiveAvg and fiveAvg >= thirtyAvg:
#         return 'OPFT'
#     if oneTwentyAvg >= price and price >= thirtyAvg and thirtyAvg >= fiveAvg:
#         return 'OPTF'
#     if oneTwentyAvg >= fiveAvg and fiveAvg >= price and price >= thirtyAvg:
#         return 'OFPT'
#     if oneTwentyAvg >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= price:
#         return 'OFTP'
#     if oneTwentyAvg >= thirtyAvg and thirtyAvg >= price and price >= fiveAvg:
#         return 'OTPF'
#     if oneTwentyAvg >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= price:
#         return 'OTFP'
#     return 'WRNG'

sSlope = {
    'D' :[],
    'L' :[],
    'F' :[],
    'H' :[],
    'U' :[]
}
fSlope = {
    'D' :[],
    'L' :[],
    'F' :[],
    'H' :[],
    'U' :[]
}
tSlope = {
    'D' :[],
    'L' :[],
    'F' :[],
    'H' :[],
    'U' :[]
}
oSlope = {
    'D' :[],
    'L' :[],
    'F' :[],
    'H' :[],
    'U' :[]
}
M = {}
previousOneTwentyAvgSet = []
previousThirtyAvgSet = []
previousFiveAvgSet = []
previousTSecAvgSet = []
for trade in trades:
    testNumber = 1

    match = False
    if first and date_ms is not None:
        first = False
        match = True
        total -= 1
    elif date_ms is not None:
        print(trade)
    price=trade[11]
    tSecAvg = trade[50]
    fiveAvg = trade[46]
    thirtyAvg = trade[47]
    oneTwentyAvg = trade[49]
    high = fixHighPrice(trade)
    low = fixLowPrice(trade)

    # thirtyToFivePercent = ( thirtyAvg * 100 / fiveAvg ) - 100
    # thirtyToOneTPercent = ( thirtyAvg * 100 / oneTwentyAvg ) - 100
    # oneTtoFivePercent = ( oneTwentyAvg * 100 / fiveAvg ) - 100
    if previousFiveAvg is None:
        oneTwentySlope = 0
        thirtySlope = 0
        fiveSlope = 0
        tSecSlope = 0
    else:
        oneTwentySlope = ((oneTwentyAvg - previousOneTwentyAvg) / (2 - 1))
        thirtySlope = ((thirtyAvg - previousThirtyAvg) / (2 - 1))
        fiveSlope = ((fiveAvg - previousFiveAvg) / (2 - 1))
        tSecSlope = ((tSecAvg - previousTSecAvg) / (2 - 1))

    likelihood = tradeLikelyhood(trade, high, low)

    slopeString = categories.getSlopeString(tSecSlope, fiveSlope, thirtySlope, oneTwentySlope)
    rollingString = categories.getRollingString(price, tSecAvg, fiveAvg, thirtyAvg, oneTwentyAvg)

    sSlope[slopeString[0]].append(tSecSlope)
    fSlope[slopeString[1]].append(fiveSlope)
    tSlope[slopeString[2]].append(thirtySlope)
    oSlope[slopeString[3]].append(oneTwentySlope)

    if rollingString+'-'+slopeString not in M:
        M[rollingString+'-'+slopeString] = []

    M[rollingString+'-'+slopeString].append(likelihood)

    total += 1

    previousOneTwentyAvgSet.append(oneTwentyAvg)
    previousThirtyAvgSet.append(thirtyAvg)
    previousFiveAvgSet.append(fiveAvg)
    previousTSecAvgSet.append(tSecAvg)
    while len(previousOneTwentyAvgSet) > 15:
        previousOneTwentyAvg = previousOneTwentyAvgSet.pop(0)
        previousThirtyAvg = previousThirtyAvgSet.pop(0)
        previousFiveAvg = previousFiveAvgSet.pop(0)
        previousTSecAvg = previousTSecAvgSet.pop(0)

mTotal = 0

def sortCategories(a, b):
    firstTot = len(a)
    secondTot = len(b)
    firstAvg = 0.0
    secondAvg = 0.0
    if firstTot > 0:
        firstAvg = sum(a) / firstTot
    if secondTot > 0:
        secondAvg = sum(b) / secondTot
    if firstAvg > secondAvg:
        return 1
    elif firstAvg == secondAvg:
        return 0
    else:
        return -1

M = {key: value for key, value in sorted(M.items(), key=lambda item: ( 0 if (not len(item[1])) else (sum(item[1]) / len(item[1]))) )}

for rank in M:
    thisTot = len(M[rank])
    mTotal += thisTot
    avg = 0
    thisPercent = 0.0
    tMax = 0
    tMin = 0
    mid = 0
    if thisTot > 0:
        avg = sum(M[rank]) / thisTot
        mid = M[rank][round(thisTot / 2)]
        tMin = min(M[rank])
        tMax = max(M[rank])
    cur.execute(
            'REPLACE INTO predictions VALUES ("{0:s}",{1:0.8f})'.format(rank, avg))
    conn.commit()

    print('{0:s} min: {1:5.2f}     mid: {2:5.2f}     max: {3:5.2f}     count: {4:6d} avgL: {5:4.2f}'.format(rank, tMin, mid, tMax, thisTot, avg))

print('mTotal {0:d} total {1:d}'.format(mTotal, total))

print("30 sec")
for slopes in sSlope:
    thisTot = len(sSlope[slopes])
    mTotal += thisTot
    avg = 0
    thisPercent = 0.0
    tMax = 0
    tMin = 0
    mid = 0
    if thisTot > 0:
        avg = sum(sSlope[slopes]) / thisTot
        mid = sSlope[slopes][round(thisTot / 2)]
        tMin = min(sSlope[slopes])
        tMax = max(sSlope[slopes])
    print('{0:s} min: {1:5.2f}     mid: {2:5.2f}     max: {3:5.2f}     count: {4:6d} avgL: {5:4.2f}'.format(slopes, tMin, mid, tMax, thisTot, avg))

print("5 min")
for slopes in fSlope:
    thisTot = len(fSlope[slopes])
    mTotal += thisTot
    avg = 0
    thisPercent = 0.0
    tMax = 0
    tMin = 0
    mid = 0
    if thisTot > 0:
        avg = sum(fSlope[slopes]) / thisTot
        mid = fSlope[slopes][round(thisTot / 2)]
        tMin = min(fSlope[slopes])
        tMax = max(fSlope[slopes])
    print('{0:s} min: {1:5.2f}     mid: {2:5.2f}     max: {3:5.2f}     count: {4:6d} avgL: {5:4.2f}'.format(slopes, tMin, mid, tMax, thisTot, avg))

print("30 min")
for slopes in tSlope:
    thisTot = len(tSlope[slopes])
    mTotal += thisTot
    avg = 0
    thisPercent = 0.0
    tMax = 0
    tMin = 0
    mid = 0
    if thisTot > 0:
        avg = sum(tSlope[slopes]) / thisTot
        mid = tSlope[slopes][round(thisTot / 2)]
        tMin = min(tSlope[slopes])
        tMax = max(tSlope[slopes])
    print('{0:s} min: {1:5.2f}     mid: {2:5.2f}     max: {3:5.2f}     count: {4:6d} avgL: {5:4.2f}'.format(slopes, tMin, mid, tMax, thisTot, avg))

print("120 min")
for slopes in oSlope:
    thisTot = len(oSlope[slopes])
    mTotal += thisTot
    avg = 0
    thisPercent = 0.0
    tMax = 0
    tMin = 0
    mid = 0
    if thisTot > 0:
        avg = sum(oSlope[slopes]) / thisTot
        mid = oSlope[slopes][round(thisTot / 2)]
        tMin = min(oSlope[slopes])
        tMax = max(oSlope[slopes])
    print('{0:s} min: {1:5.2f}     mid: {2:5.2f}     max: {3:5.2f}     count: {4:6d} avgL: {5:4.2f}'.format(slopes, tMin, mid, tMax, thisTot, avg))


# print(sSlope)
# print(fSlope)
# print(tSlope)
# print(oSlope)

# M['OPFT-HHF'].sort()
# M['PFOT-HHF'].sort()
# plt.plot(M['OPFT-HHF'], label='OPFT-HHF')
# plt.plot(M['PFOT-HHF'], label='PFOT-HHF')
# plt.show()