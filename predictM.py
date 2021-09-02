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
import numpy as np

import categories

market = 'BTCUSDT'

date_ms = None
if len(sys.argv) > 1:
    date_ms = sys.argv[1]

dateExcludes = 'AND (( date < 1615989880 ) OR ( date > 1616004584 AND  date < 1616073970 ) OR ( date > 1616088770 AND  date < 1616240913 ) OR ( date > 1616257108 AND  date < 1616243798 ) OR ( date > 1616282088 AND  date < 1616268855 ) OR ( date > 1616284889 AND  date < 1616281056 ) OR ( date > 1616296324 AND  date < 1616282393 ) OR ( date > 1616297171 AND  date < 1616284491 ) OR ( date > 1616299532 AND  date < 1616386037 ) OR ( date > 1616401144 AND  date < 1616600685 ) OR ( date > 1616615807 AND  date < 1616607621 ) OR ( date > 1616625310 AND  date < 1616713436 ) OR ( date > 1616729592 AND  date < 1616727603 ) OR ( date > 1616742883 AND  date < 1616728883 ) OR ( date > 1616744343 AND  date < 1616904517 ) OR ( date > 1616919340 AND  date < 1617232403 ) OR ( date > 1617247124 AND  date < 1618060128 ) OR ( date > 1618085229 AND  date < 1618075523 ) OR ( date > 1618091303 AND  date < 1618258896 ) OR ( date > 1618274915 AND  date < 1618432199 ) OR ( date > 1618691523 AND  date < 1618939428 ) OR ( date > 1618954162 AND  date < 1618940011 ) OR ( date > 1618955402 AND  date < 1618984798 ) OR ( date > 1619000153 AND  date < 1620399655 ) OR ( date > 1620418867 AND  date < 1620450052 ) OR ( date > 1620465447 AND  date < 1620453712 ) OR ( date > 1620469381 AND  date < 1620488396 ) OR ( date > 1620504974 AND  date < 1620804232 ) OR ( date > 1620819355 AND  date < 1620902557 ) OR ( date > 1620917271 AND  date < 1623434900 ) OR ( date > 1623449843 AND  date < 1623594379 ) OR ( date > 1623610097 AND  date < 1624378640 ) OR ( date > 1624461262 ))'
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
    cur.execute("SELECT p_oneTwentyMin_changePercent,p_sixtyMin_changePercent,p_thirtyMin_changePercent,p_fifteenMin_changePercent,p_tenMin_changePercent,p_fiveMin_changePercent,p_threeMin_changePercent,p_oneMin_changePercent,p_thirtySec_changePercent,p_tenSec_changePercent,p_fiveSec_changePercent,price,f_fiveSec_changePercent,f_tenSec_changePercent,f_thirtySec_changePercent,f_oneMin_changePercent,f_threeMin_changePercent,f_fiveMin_changePercent,f_tenMin_changePercent,f_fifteenMin_changePercent,f_thirtyMin_changePercent,f_sixtyMin_changePercent,f_oneTwentyMin_changePercent, date_ms,f_fiveSec_lowPrice,f_tenSec_lowPrice,f_thirtySec_lowPrice,f_oneMin_lowPrice,f_threeMin_lowPrice,f_fiveMin_lowPrice,f_tenMin_lowPrice,f_fifteenMin_lowPrice,f_thirtyMin_lowPrice,f_sixtyMin_lowPrice,f_oneTwentyMin_lowPrice,f_fiveSec_highPrice,f_tenSec_highPrice,f_thirtySec_highPrice,f_oneMin_highPrice,f_threeMin_highPrice,f_fiveMin_highPrice,f_tenMin_highPrice,f_fifteenMin_highPrice,f_thirtyMin_highPrice,f_sixtyMin_highPrice,f_oneTwentyMin_highPrice,p_fiveMin_avgPrice,p_thirtyMin_avgPrice,p_fiveSec_avgPrice,p_oneTwentyMin_avgPrice, p_thirtySec_avgPrice FROM trades WHERE market = '{0:s}' AND p_oneTwentyMin_changePercent IS NOT NULL AND p_sixtyMin_changePercent IS NOT NULL AND p_thirtyMin_changePercent IS NOT NULL AND p_fifteenMin_changePercent IS NOT NULL AND p_tenMin_changePercent IS NOT NULL AND p_fiveMin_changePercent IS NOT NULL AND p_threeMin_changePercent IS NOT NULL AND p_oneMin_changePercent IS NOT NULL AND p_thirtySec_changePercent IS NOT NULL AND p_tenSec_changePercent IS NOT NULL AND p_fiveSec_changePercent IS NOT NULL AND price IS NOT NULL AND f_fiveSec_changePercent IS NOT NULL AND f_tenSec_changePercent IS NOT NULL AND f_thirtySec_changePercent IS NOT NULL AND f_oneMin_changePercent IS NOT NULL AND f_threeMin_changePercent IS NOT NULL AND f_fiveMin_changePercent IS NOT NULL AND f_tenMin_changePercent IS NOT NULL AND f_fifteenMin_changePercent IS NOT NULL AND f_thirtyMin_changePercent IS NOT NULL AND f_sixtyMin_changePercent IS NOT NULL AND f_oneTwentyMin_changePercent IS NOT NULL AND f_oneTwentyMin_lowPrice IS NOT NULL {1:s} ORDER BY date_ms ASC limit 1".format(market, dateExcludes))
else:
    cur.execute("SELECT p_oneTwentyMin_changePercent,p_sixtyMin_changePercent,p_thirtyMin_changePercent,p_fifteenMin_changePercent,p_tenMin_changePercent,p_fiveMin_changePercent,p_threeMin_changePercent,p_oneMin_changePercent,p_thirtySec_changePercent,p_tenSec_changePercent,p_fiveSec_changePercent,price,f_fiveSec_changePercent,f_tenSec_changePercent,f_thirtySec_changePercent,f_oneMin_changePercent,f_threeMin_changePercent,f_fiveMin_changePercent,f_tenMin_changePercent,f_fifteenMin_changePercent,f_thirtyMin_changePercent,f_sixtyMin_changePercent,f_oneTwentyMin_changePercent, date_ms,f_fiveSec_lowPrice,f_tenSec_lowPrice,f_thirtySec_lowPrice,f_oneMin_lowPrice,f_threeMin_lowPrice,f_fiveMin_lowPrice,f_tenMin_lowPrice,f_fifteenMin_lowPrice,f_thirtyMin_lowPrice,f_sixtyMin_lowPrice,f_oneTwentyMin_lowPrice,f_fiveSec_highPrice,f_tenSec_highPrice,f_thirtySec_highPrice,f_oneMin_highPrice,f_threeMin_highPrice,f_fiveMin_highPrice,f_tenMin_highPrice,f_fifteenMin_highPrice,f_thirtyMin_highPrice,f_sixtyMin_highPrice,f_oneTwentyMin_highPrice,p_fiveMin_avgPrice,p_thirtyMin_avgPrice,p_fiveSec_avgPrice,p_oneTwentyMin_avgPrice, p_thirtySec_avgPrice FROM trades WHERE market = '{0:s}' AND date_ms <= '{1:s}' AND p_oneTwentyMin_changePercent IS NOT NULL AND p_sixtyMin_changePercent IS NOT NULL AND p_thirtyMin_changePercent IS NOT NULL AND p_fifteenMin_changePercent IS NOT NULL AND p_tenMin_changePercent IS NOT NULL AND p_fiveMin_changePercent IS NOT NULL AND p_threeMin_changePercent IS NOT NULL AND p_oneMin_changePercent IS NOT NULL AND p_thirtySec_changePercent IS NOT NULL AND p_tenSec_changePercent IS NOT NULL AND p_fiveSec_changePercent IS NOT NULL AND price IS NOT NULL AND f_fiveSec_changePercent IS NOT NULL AND f_tenSec_changePercent IS NOT NULL AND f_thirtySec_changePercent IS NOT NULL AND f_oneMin_changePercent IS NOT NULL AND f_threeMin_changePercent IS NOT NULL AND f_fiveMin_changePercent IS NOT NULL AND f_tenMin_changePercent IS NOT NULL AND f_fifteenMin_changePercent IS NOT NULL AND f_thirtyMin_changePercent IS NOT NULL AND f_sixtyMin_changePercent IS NOT NULL AND f_oneTwentyMin_changePercent IS NOT NULL AND f_oneTwentyMin_lowPrice IS NOT NULL ORDER BY date_ms DESC LIMIT 2".format(market, date_ms))


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
    # likelihood += (high[0] / sellPercent)
    likelihood += (high[1] / sellPercent)
    # likelihood += (high[2] / sellPercent)
    # likelihood += (high[3] / sellPercent)
    # likelihood += (high[4] / sellPercent)
    # likelihood += (high[5] / sellPercent)
    # likelihood += (high[6] / sellPercent) # 10 min or less ^
    # likelihood += (high[7] / sellPercent)
    # likelihood += (high[8] / sellPercent)
    # likelihood += (high[9] / sellPercent)
    # likelihood += (high[10] / sellPercent)
    # likelihood += (low[0] / sellPercent)
    likelihood += (low[1] / sellPercent)
    # likelihood += (low[2] / sellPercent)
    # likelihood += (low[3] / sellPercent)
    # likelihood += (low[4] / sellPercent)
    # likelihood += (low[5] / sellPercent)
    # likelihood += (low[6] / sellPercent) # 10 min or less ^
    # likelihood += (low[7] / sellPercent)
    # likelihood += (low[8] / sellPercent)
    # likelihood += (low[9] / sellPercent)
    # likelihood += (low[10] / sellPercent)

    return likelihood
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
def changetSec(trade, high, low):
    # return trade[14]
    return high[2] + low[2]
    
def changeFive(trade, high, low):
    # return trade[17]
    return high[5] + low[5]

def changeThirty(trade, high, low):
    # return trade[20]
    return high[8] + low[8]

def changeOneTwenty(trade, high, low):
    # return trade[22]
    return high[10] + low[10]

def tradeLikelyhoodLongTerm(trade, high, low):
    likelihood = 0;
    # likelihood += (high[0] / sellPercent)
    # likelihood += (high[1] / sellPercent)
    # likelihood += (high[2] / sellPercent)
    # likelihood += (high[3] / sellPercent)
    # likelihood += (high[4] / sellPercent)
    # likelihood += (high[5] / sellPercent)
    # likelihood += (high[6] / sellPercent) # 10 min or less ^
    # likelihood += (high[7] / sellPercent)
    # likelihood += (high[8] / sellPercent)
    # likelihood += (high[9] / sellPercent)
    likelihood += (high[10] / sellPercent)
    # likelihood += (low[0] / sellPercent)
    # likelihood += (low[1] / sellPercent)
    # likelihood += (low[2] / sellPercent)
    # likelihood += (low[3] / sellPercent)
    # likelihood += (low[4] / sellPercent)
    # likelihood += (low[5] / sellPercent)
    # likelihood += (low[6] / sellPercent) # 10 min or less ^
    # likelihood += (low[7] / sellPercent)
    # likelihood += (low[8] / sellPercent)
    # likelihood += (low[9] / sellPercent)
    likelihood += (low[10] / sellPercent)

    # Adjust extreme edge cases
    if likelihood > 50:
        return 50
    if likelihood < -50:
        return -50

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

sellPercent=0.14
buyPercent=-0.14

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

sSlope = {}
fSlope = {}
tSlope = {}
oSlope = {}
S = {}
F = {}
T = {}
O = {}
previousOneTwentyAvgSet = []
previousThirtyAvgSet = []
previousFiveAvgSet = []
previousTSecAvgSet = []
last_ms = 0

batchNo=0
python3 predictMscore.py
if date_ms is not None:
    trades = cur.fetchall()
    trades = trades[::-1]
else:
    while True:
        trades = cur.fetchmany(100000)
        print(trades)
        exit();

        print("Batch {0}".format(batchNo))
        batchNo = batchNo + 1
        if trades == None:
            print(" DONE ")
            break
        for trade in trades:
            if trade[23] - 300000 > last_ms:
                previousOneTwentyAvgSet = []
                previousThirtyAvgSet = []
                previousFiveAvgSet = []
                previousTSecAvgSet = []   
            
            testNumber = 1

            match = False
            if first and date_ms is not None:
                first = False
                match = True
                total -= 1
            # elif date_ms is not None:
                # print(trade)
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

            slopeString = categories.getSlopeString(tSecSlope, fiveSlope, thirtySlope, oneTwentySlope)
            rollingString = categories.getRollingString(price, tSecAvg, fiveAvg, thirtyAvg, oneTwentyAvg)

            if slopeString[0] not in sSlope:
                sSlope[slopeString[0]] = []
            sSlope[slopeString[0]].append(tSecSlope)
            if slopeString[1] not in fSlope:
                fSlope[slopeString[1]] = []
            fSlope[slopeString[1]].append(fiveSlope)
            if slopeString[2] not in tSlope:
                tSlope[slopeString[2]] = []
            tSlope[slopeString[2]].append(thirtySlope)
            if slopeString[3] not in oSlope:
                oSlope[slopeString[3]] = []
            oSlope[slopeString[3]].append(oneTwentySlope)

            # if 'TOFSP-QLLC' == rollingString+'-'+slopeString:
            #     print(trade[23])
            if len(previousOneTwentyAvgSet) == 15:
                if rollingString+'-'+slopeString not in S:
                    S[rollingString+'-'+slopeString] = []
                    F[rollingString+'-'+slopeString] = []
                    T[rollingString+'-'+slopeString] = []
                    O[rollingString+'-'+slopeString] = []

                change = changetSec(trade, high, low)
                if change not in S[rollingString+'-'+slopeString]:
                    S[rollingString+'-'+slopeString].append(change)
                change = changeFive(trade, high, low)
                if change not in F[rollingString+'-'+slopeString]:
                    F[rollingString+'-'+slopeString].append(change)
                change = changeThirty(trade, high, low)
                if change not in T[rollingString+'-'+slopeString]:
                    T[rollingString+'-'+slopeString].append(change)
                change = changeOneTwenty(trade, high, low)
                if change not in O[rollingString+'-'+slopeString]:
                    O[rollingString+'-'+slopeString].append(change)

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

            last_ms = trade[23]

trades=None

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

S = {key: value for key, value in sorted(S.items(), key=lambda item: len(item[1]) )}


# S = {key: value for key, value in sorted(S.items(), key=lambda item: ( 0 if (not len(item[1])) else ( max(0, ((sum(item[1]) / len(item[1])) - np.std(item[1]))) if ( ( sum(item[1]) / len(item[1]) ) > 0 ) else ( min(0,( sum(item[1]) / len(item[1]) ) + np.std(item[1]) ) ) ) ) )}

gtFiveTot = 0
gtFiveLong = 0
gtFiveShort = 0

for rank in S:
    thisTot = len(S[rank])
    mTotal += thisTot

    Savg = 0
    SthisPercent = 0.0
    StMax = 0
    StMin = 0
    Smid = 0
    if thisTot > 0:
        Sstd = np.std(S[rank])
        Savg = sum(S[rank]) / thisTot
        # Smid = S[rank][round(thisTot / 2)]
        StMin = min(S[rank])
        StMax = max(S[rank])
        SbetAvg = max(0, (Savg - ( 0.25 * Sstd ))) if ( sum(S[rank]) > 0 ) else min(0, ( Savg + ( 0.25 * Sstd ) ) )
    # if thisTot > 10 and SbetAvg == 0.0:
    # if rank == 'TOFSP-QLLC':
    #     print('{0:s} count: {1:6d}   min: {2:5.3f}  max: {4:5.3f}   avg: {5:4.2f}   betAvg: {6:5.3f}   std:{7:5.3f}'.format(rank, thisTot, StMin, 'Smid', StMax, Savg, SbetAvg, Sstd, ))

    thisTot = len(F[rank])
    Favg = 0
    FthisPercent = 0.0
    FtMax = 0
    FtMin = 0
    Fmid = 0
    if thisTot > 0:
        Fstd = np.std(F[rank])
        Favg = sum(F[rank]) / thisTot
        # Fmid = F[rank][round(thisTot / 2)]
        FtMin = min(F[rank])
        FtMax = max(F[rank])
        FbetAvg = max(0, (Favg - ( 0.25 * Fstd ))) if ( sum(F[rank]) > 0 ) else min(0, ( Favg + ( 0.25 * Fstd ) ) )

    thisTot = len(T[rank])
    Tavg = 0
    TthisPercent = 0.0
    TtMax = 0
    TtMin = 0
    Tmid = 0
    if thisTot > 0:
        Tstd = np.std(T[rank])
        Tavg = sum(T[rank]) / thisTot
        # Tmid = T[rank][round(thisTot / 2)]
        TtMin = min(T[rank])
        TtMax = max(T[rank])
        TbetAvg = max(0, (Tavg - ( 0.25 * Tstd ))) if ( sum(T[rank]) > 0 ) else min(0, ( Tavg + ( 0.25 * Tstd ) ) )

    thisTot = len(O[rank])
    Oavg = 0
    OthisPercent = 0.0
    OtMax = 0
    OtMin = 0
    Omid = 0
    if thisTot > 0:
        Ostd = np.std(O[rank])
        Oavg = sum(O[rank]) / thisTot
        # Omid = O[rank][round(thisTot / 2)]
        OtMin = min(O[rank])
        OtMax = max(O[rank])
        ObetAvg = max(0, (Oavg - ( 0.25 * Ostd ))) if ( sum(O[rank]) > 0 ) else min(0, ( Oavg + ( 0.25 * Ostd ) ) )

    if len(S[rank]) >= 3 or len(F[rank]) >= 3 or len(T[rank]) >= 3 or len(O[rank]) >= 3:
        cur.execute(
                'REPLACE INTO predictions VALUES ("{0:s}",{1:0.8f}, {2:0.8f}, {3:0.8f}, {4:0.8f})'.format(rank, SbetAvg, FbetAvg, TbetAvg, ObetAvg))
        conn.commit()

    if len(S[rank]) >= 3 or len(F[rank]) >= 3 or len(T[rank]) >= 3 or len(O[rank]) >= 3:
        gtFiveTot += 1

        # print('{0:s} count: {1:6d}   -: {2:5.3f}  +: {4:5.3f}   a: {5:4.2f}   b: {6:5.3f}   s:{7:5.3f}    -: {8:5.3f}   +: {10:5.3f}   a: {11:4.2f}   b: {12:5.3f}   s:{13:5.3f}    -: {14:5.3f}   +: {16:5.3f}   a: {17:4.2f}   b: {18:5.3f}   s:{19:5.3f}    -: {20:5.3f}   +: {22:5.3f}   a: {23:4.2f}   b: {24:5.3f}   s:{25:5.3f} '.format(rank, thisTot, StMin, 'Smid', Savg, StMax, SbetAvg, Sstd, FtMin, 'Fmid', Favg, FtMax, FbetAvg, Fstd, TtMin, 'Tmid', Tavg, TtMax, TbetAvg, Tstd, OtMin, 'Omid', Oavg, OtMax, ObetAvg, Ostd))
        # print('{0:s} count: {1:6d}   -: {2:5.3f}   : {3:5.3f}   +: {4:5.3f}   a: {5:4.2f}   b: {6:5.3f}   s:{7:5.3f}    -: {8:5.3f}   : {9:5.3f}   +: {10:5.3f}   a: {11:4.2f}   b: {12:5.3f}   s:{13:5.3f}    -: {14:5.3f}   : {15:5.3f}   +: {16:5.3f}   a: {17:4.2f}   b: {18:5.3f}   s:{19:5.3f}    -: {20:5.3f}   : {21:5.3f}   +: {22:5.3f}   a: {23:4.2f}   b: {24:5.3f}   s:{25:5.3f} '.format(rank, thisTot, StMin, Smid, Savg, StMax, SbetAvg, Sstd, FtMin, Fmid, Favg, FtMax, FbetAvg, Fstd, TtMin, Tmid, Tavg, TtMax, TbetAvg, Tstd, OtMin, Omid, Oavg, OtMax, ObetAvg, Ostd))

print('mTotal {0:d} total {1:d} gt5 {2:d} gt5short {3:d} gt5long {4:d}'.format(mTotal, total, gtFiveTot, gtFiveShort, gtFiveLong))

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
    print('{0:s} min: {1:5.3f}     mid: {2:5.3f}     max: {3:5.3f}     count: {4:6d} avgL: {5:4.2f}'.format(slopes, tMin, mid, tMax, thisTot, avg))

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
    print('{0:s} min: {1:5.3f}     mid: {2:5.3f}     max: {3:5.3f}     count: {4:6d} avgL: {5:4.2f}'.format(slopes, tMin, mid, tMax, thisTot, avg))

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
    print('{0:s} min: {1:5.3f}     mid: {2:5.3f}     max: {3:5.3f}     count: {4:6d} avgL: {5:4.2f}'.format(slopes, tMin, mid, tMax, thisTot, avg))

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
    print('{0:s} min: {1:5.3f}     mid: {2:5.3f}     max: {3:5.3f}     count: {4:6d} avgL: {5:4.2f}'.format(slopes, tMin, mid, tMax, thisTot, avg))


# print(sSlope)
# print(fSlope)
# print(tSlope)
# print(oSlope)

# M['OPFT-HHF'].sort()
# M['PFOT-HHF'].sort()
# plt.plot(M['OPFT-HHF'], label='OPFT-HHF')
# plt.plot(M['PFOT-HHF'], label='PFOT-HHF')
# plt.show()