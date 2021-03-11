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
    cur.execute("SELECT p_oneTwentyMin_changePercent,p_sixtyMin_changePercent,p_thirtyMin_changePercent,p_fifteenMin_changePercent,p_tenMin_changePercent,p_fiveMin_changePercent,p_threeMin_changePercent,p_oneMin_changePercent,p_thirtySec_changePercent,p_tenSec_changePercent,p_fiveSec_changePercent,price,f_fiveSec_changePercent,f_tenSec_changePercent,f_thirtySec_changePercent,f_oneMin_changePercent,f_threeMin_changePercent,f_fiveMin_changePercent,f_tenMin_changePercent,f_fifteenMin_changePercent,f_thirtyMin_changePercent,f_sixtyMin_changePercent,f_oneTwentyMin_changePercent, date_ms,f_fiveSec_lowPrice,f_tenSec_lowPrice,f_thirtySec_lowPrice,f_oneMin_lowPrice,f_threeMin_lowPrice,f_fiveMin_lowPrice,f_tenMin_lowPrice,f_fifteenMin_lowPrice,f_thirtyMin_lowPrice,f_sixtyMin_lowPrice,f_oneTwentyMin_lowPrice,f_fiveSec_highPrice,f_tenSec_highPrice,f_thirtySec_highPrice,f_oneMin_highPrice,f_threeMin_highPrice,f_fiveMin_highPrice,f_tenMin_highPrice,f_fifteenMin_highPrice,f_thirtyMin_highPrice,f_sixtyMin_highPrice,f_oneTwentyMin_highPrice,p_fiveMin_avgPrice,p_thirtyMin_avgPrice,p_fiveSec_avgPrice,p_oneTwentyMin_avgPrice FROM trades WHERE market = '{0:s}' AND p_oneTwentyMin_changePercent IS NOT NULL AND p_sixtyMin_changePercent IS NOT NULL AND p_thirtyMin_changePercent IS NOT NULL AND p_fifteenMin_changePercent IS NOT NULL AND p_tenMin_changePercent IS NOT NULL AND p_fiveMin_changePercent IS NOT NULL AND p_threeMin_changePercent IS NOT NULL AND p_oneMin_changePercent IS NOT NULL AND p_thirtySec_changePercent IS NOT NULL AND p_tenSec_changePercent IS NOT NULL AND p_fiveSec_changePercent IS NOT NULL AND price IS NOT NULL AND f_fiveSec_changePercent IS NOT NULL AND f_tenSec_changePercent IS NOT NULL AND f_thirtySec_changePercent IS NOT NULL AND f_oneMin_changePercent IS NOT NULL AND f_threeMin_changePercent IS NOT NULL AND f_fiveMin_changePercent IS NOT NULL AND f_tenMin_changePercent IS NOT NULL AND f_fifteenMin_changePercent IS NOT NULL AND f_thirtyMin_changePercent IS NOT NULL AND f_sixtyMin_changePercent IS NOT NULL AND f_oneTwentyMin_changePercent IS NOT NULL AND f_oneTwentyMin_lowPrice IS NOT NULL ORDER BY date_ms ASC".format(market, startTime, endTime))
else:
    cur.execute("SELECT p_oneTwentyMin_changePercent,p_sixtyMin_changePercent,p_thirtyMin_changePercent,p_fifteenMin_changePercent,p_tenMin_changePercent,p_fiveMin_changePercent,p_threeMin_changePercent,p_oneMin_changePercent,p_thirtySec_changePercent,p_tenSec_changePercent,p_fiveSec_changePercent,price,f_fiveSec_changePercent,f_tenSec_changePercent,f_thirtySec_changePercent,f_oneMin_changePercent,f_threeMin_changePercent,f_fiveMin_changePercent,f_tenMin_changePercent,f_fifteenMin_changePercent,f_thirtyMin_changePercent,f_sixtyMin_changePercent,f_oneTwentyMin_changePercent, date_ms,f_fiveSec_lowPrice,f_tenSec_lowPrice,f_thirtySec_lowPrice,f_oneMin_lowPrice,f_threeMin_lowPrice,f_fiveMin_lowPrice,f_tenMin_lowPrice,f_fifteenMin_lowPrice,f_thirtyMin_lowPrice,f_sixtyMin_lowPrice,f_oneTwentyMin_lowPrice,f_fiveSec_highPrice,f_tenSec_highPrice,f_thirtySec_highPrice,f_oneMin_highPrice,f_threeMin_highPrice,f_fiveMin_highPrice,f_tenMin_highPrice,f_fifteenMin_highPrice,f_thirtyMin_highPrice,f_sixtyMin_highPrice,f_oneTwentyMin_highPrice,p_fiveMin_avgPrice,p_thirtyMin_avgPrice,p_fiveSec_avgPrice,p_oneTwentyMin_avgPrice FROM trades WHERE market = '{0:s}' AND date_ms <= '{1:s}' AND p_oneTwentyMin_changePercent IS NOT NULL AND p_sixtyMin_changePercent IS NOT NULL AND p_thirtyMin_changePercent IS NOT NULL AND p_fifteenMin_changePercent IS NOT NULL AND p_tenMin_changePercent IS NOT NULL AND p_fiveMin_changePercent IS NOT NULL AND p_threeMin_changePercent IS NOT NULL AND p_oneMin_changePercent IS NOT NULL AND p_thirtySec_changePercent IS NOT NULL AND p_tenSec_changePercent IS NOT NULL AND p_fiveSec_changePercent IS NOT NULL AND price IS NOT NULL AND f_fiveSec_changePercent IS NOT NULL AND f_tenSec_changePercent IS NOT NULL AND f_thirtySec_changePercent IS NOT NULL AND f_oneMin_changePercent IS NOT NULL AND f_threeMin_changePercent IS NOT NULL AND f_fiveMin_changePercent IS NOT NULL AND f_tenMin_changePercent IS NOT NULL AND f_fifteenMin_changePercent IS NOT NULL AND f_thirtyMin_changePercent IS NOT NULL AND f_sixtyMin_changePercent IS NOT NULL AND f_oneTwentyMin_changePercent IS NOT NULL AND f_oneTwentyMin_lowPrice IS NOT NULL ORDER BY date_ms DESC LIMIT 2".format(market, date_ms))



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
M = {
    'PFTO': [],
    'PFOT': [],
    'PTFO': [],
    'PTOF': [],
    'POFT': [],
    'POTF': [],
    'FPTO': [],
    'FPOT': [],
    'FTPO': [],
    'FTOP': [],
    'FOPT': [],
    'FOTP': [],
    'TPFO': [],
    'TPOF': [],
    'TFPO': [],
    'TFOP': [],
    'TOPF': [],
    'TOFP': [],
    'OPFT': [],
    'OPTF': [],
    'OFPT': [],
    'OFTP': [],
    'OTPF': [],
    'OTFP': [],
    'WRONG': []
}
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
    fiveAvg = trade[46]
    thirtyAvg = trade[47]
    oneTwentyAvg = trade[49]
    high = fixHighPrice(trade)
    low = fixLowPrice(trade)

    likelihood = tradeLikelyhood(trade, high, low)

    if price >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg:
        M['PFTO'].append(likelihood)
    elif price >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg:
        M['PFOT'].append(likelihood)
    elif price >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= oneTwentyAvg:
        M['PTFO'].append(likelihood)
    elif price >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg:
        M['PTOF'].append(likelihood)
    elif price >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= thirtyAvg:
        M['POFT'].append(likelihood)
    elif price >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= fiveAvg:
        M['POTF'].append(likelihood)

    elif fiveAvg >= price and price >= thirtyAvg and thirtyAvg >= oneTwentyAvg:
        M['FPTO'].append(likelihood)
    elif fiveAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg:
        M['FPOT'].append(likelihood)
    elif fiveAvg >= thirtyAvg and thirtyAvg >= price and price >= oneTwentyAvg:
        M['FTPO'].append(likelihood)
    elif fiveAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= price:
        M['FTOP'].append(likelihood)
    elif fiveAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= thirtyAvg:
        M['FOPT'].append(likelihood)
    elif fiveAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= price:
        M['FOTP'].append(likelihood)

    elif thirtyAvg >= price and price >= fiveAvg and fiveAvg >= oneTwentyAvg:
        M['TPFO'].append(likelihood)
    elif thirtyAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= fiveAvg:
        M['TPOF'].append(likelihood)
    elif thirtyAvg >= fiveAvg and fiveAvg >= price and price >= oneTwentyAvg:
        M['TFPO'].append(likelihood)
    elif thirtyAvg >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= price:
        M['TFOP'].append(likelihood)
    elif thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= fiveAvg:
        M['TOPF'].append(likelihood)
    elif thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= price:
        M['TOFP'].append(likelihood)

    elif oneTwentyAvg >= price and price >= fiveAvg and fiveAvg >= thirtyAvg:
        M['OPFT'].append(likelihood)
    elif oneTwentyAvg >= price and price >= thirtyAvg and thirtyAvg >= fiveAvg:
        M['OPTF'].append(likelihood)
    elif oneTwentyAvg >= fiveAvg and fiveAvg >= price and price >= thirtyAvg:
        M['OFPT'].append(likelihood)
    elif oneTwentyAvg >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= price:
        M['OFTP'].append(likelihood)
    elif oneTwentyAvg >= thirtyAvg and thirtyAvg >= price and price >= fiveAvg:
        M['OTPF'].append(likelihood)
    elif oneTwentyAvg >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= price:
        M['OTFP'].append(likelihood)

    else:
        print('WRONG: '+str(likelihood))
    total += 1


mTotal = 0

for rank in M:
    thisTot = len(M[rank])
    mTotal += thisTot
    avg = 0
    thisPercent = 0.0
    if thisTot > 0:
        avg = sum(M[rank]) / thisTot
        thisPercent = thisTot * 100 / total

    print('{0:s} count: {1:6d} p: {2:6.2f}% avgL: {3:6.2f}'.format(rank, thisTot, thisPercent, avg))

print('mTotal {0:d} total {1:d}'.format(mTotal, total))