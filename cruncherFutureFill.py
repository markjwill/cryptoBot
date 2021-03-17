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

market = 'BTCUSDT'

# startTimestamp=1614663293

#restart
startTimestamp=1614774240 #Wed Mar 03 2021 12:24:00 GMT+0000
# endTimestamp=1614783293 #Wed Mar 03 2021 14:54:53 GMT+0000

# startTimestamp=1614783294 #Wed Mar 03 2021 14:54:54 GMT+0000
# endTimestamp=1614921971 #Fri Mar 05 2021 05:26:11 GMT+0000
endTimestamp=int(datetime.now().timestamp())

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

def getRawTrades(market, end_ms, seconds):
    pastTime = math.floor(end_ms + (seconds * 1000 ))
    cur.execute("SELECT price, amount, type FROM trades WHERE date_ms < {0:d} AND date_ms > {1:d} ORDER BY date_ms DESC".format(pastTime, end_ms))
    trades = cur.fetchall()

    if not len(trades):
        print("No rows found, selecting most recent")
        cur.execute("SELECT price, amount, type FROM trades WHERE date_ms < {0:d} ORDER BY date_ms DESC LIMIT 1".format(end_ms))
        trades = cur.fetchall()

    return trades

def getCalcTrades(market, period, id_col, end_ms):
    if period == 'fiveSec':
        seconds = 5
    elif period == 'tenSec':
        seconds = 10
    elif period == 'thirtySec':
        seconds = 30
    elif period == 'oneMin':
        seconds = 60
    elif period == 'threeMin':
        seconds = 180
    elif period == 'fiveMin':
        seconds = 300
    elif period == 'tenMin':
        seconds = 600
    elif period == 'fifteenMin':
        seconds = 900
    elif period == 'thirtyMin':
        seconds = 1800
    elif period == 'sixtyMin':
        seconds = 3600
    elif period == 'oneTwentyMin':
        seconds = 7200
    else:
        seconds = period
        period = 'period'
    trades = getRawTrades(market, end_ms, seconds)

    avgPrice = 0.0 #avg Price
    highPrice = 0.0 #high price
    lowPrice = 0.0 #low price
    startPrice = 0.0 #start price
    endPrice = 0.0 #end price, Current Price
    changeReal = 0.0 #start to end change price
    changePercent = 0.0 #start to end change price percent
    travelReal = 0.0 #max travel in price
    travelPercent = 0.0 #max travel in price percent
    volume = 0.0 #total volume
    volumePrMin = 0.0 #
    totalPrice = 0.0 #total
    tradeCount = len(trades)
    sellsPrMin = 0.0
    sells = 0
    buysPrMin = 0.0
    buys = 0

    endPrice = trades[0][0]
    startPrice = trades[-1][0]

    for trade in trades:
        if trade[0] > highPrice:
            highPrice = trade[0]

        if trade[0] < lowPrice or lowPrice == 0.0:
            lowPrice = trade[0]

        totalPrice += trade[0]
        volume += trade[1]

        if trade[2] == 'buy':
            buys += 1

        if trade[2] == 'sell':
            sells += 1

    avgPrice = totalPrice / tradeCount
    volumePrMin = volume / seconds * 60
    changeReal = endPrice - startPrice
    changePercent = changeReal * 100 / startPrice

    travelReal = highPrice - lowPrice
    travelPercent = travelReal * 100 / lowPrice

    tradesPrMin = tradeCount / seconds * 60
    buysPrMin = buys / seconds * 60
    sellsPrMin = sells / seconds * 60

    pastTime = math.floor(end_ms / 1000)

    cur.execute(
        "UPDATE trades SET f_{0:s}_changeReal = {1:0.8f}, f_{2:s}_changePercent = {3:0.8f}, f_{4:s}_endPrice = {5:0.8f}, f_{6:s}_lowPrice = {7:0.8f}, f_{8:s}_highPrice = {9:0.8f} WHERE date = {10:d}".format(period, changeReal, period, changePercent, period, endPrice, period, lowPrice, period, highPrice, pastTime))
    conn.commit()
    # print("updated "+period+" at date "+str(pastTime))

previousDate_ms = 0

while True:
    updatedTime = datetime.now() - timedelta(minutes=125)
    twoHrsAgo = updatedTime.timestamp() * 1000
    cur.execute("SELECT id, date_ms, f_fiveSec_lowPrice, f_tenSec_lowPrice, f_thirtySec_lowPrice, f_oneMin_lowPrice, f_threeMin_lowPrice, f_fiveMin_lowPrice, f_tenMin_lowPrice, f_fifteenMin_lowPrice, f_thirtyMin_lowPrice, f_sixtyMin_lowPrice, f_oneTwentyMin_lowPrice FROM trades WHERE p_oneTwentyMin_changePercent IS NOT NULL AND f_oneTwentyMin_changePercent IS NULL AND date_ms < "+str(twoHrsAgo)+" ORDER BY date_ms DESC LIMIT 1")
    rows = cur.fetchall()



    for row in rows:
        print("FutureCruncher "+datetime.fromtimestamp(math.floor(row[1] / 1000)).astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p")+" "+str(math.floor(row[1] / 1000)))
        if row[2] is None or row[2] == 0:
            getCalcTrades(market, 'fiveSec', row[0], row[1])
        if row[3] is None or row[3] == 0:
            getCalcTrades(market, 'tenSec', row[0], row[1])
        if row[4] is None or row[4] == 0:
            getCalcTrades(market, 'thirtySec', row[0], row[1])
        if row[5] is None or row[5] == 0:
            getCalcTrades(market, 'oneMin', row[0], row[1])
        if row[6] is None or row[6] == 0:
            getCalcTrades(market, 'threeMin', row[0], row[1])
        if row[7] is None or row[7] == 0:
            getCalcTrades(market, 'fiveMin', row[0], row[1])
        if row[8] is None or row[8] == 0:
            getCalcTrades(market, 'tenMin', row[0], row[1])
        if row[9] is None or row[9] == 0:
            getCalcTrades(market, 'fifteenMin', row[0], row[1])
        if row[10] is None or row[10] == 0:
            getCalcTrades(market, 'thirtyMin', row[0], row[1])
        if row[11] is None or row[11] == 0:
            getCalcTrades(market, 'sixtyMin', row[0], row[1])
        if row[12] is None or row[12] == 0:
            getCalcTrades(market, 'oneTwentyMin', row[0], row[1])

    if len(rows):
        previousDate_ms=row[1]
    print("Future Filler Takin a break.")
    time.sleep(0.5)



