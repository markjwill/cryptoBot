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
# endTimestamp=1614783293


# startTimestamp=1614783294
# endTimestamp=1614985672

# startTimestamp=1614783294
# endTimestamp=int(datetime.now().timestamp())

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

def getRawTrades(market, end_ms, seconds = 0):
    pastTime = round(end_ms - (seconds * 1000 ))
    cur.execute("SELECT price, amount, type FROM trades WHERE market = '{0:s}' AND date_ms > {1:d} AND date_ms < {2:d} ORDER BY date_ms DESC".format(market, pastTime, end_ms))
    trades = cur.fetchall()

    if not len(trades):
        print("No rows found, selecting most recent")
        cur.execute("SELECT price, amount, type FROM trades WHERE market = '{0:s}' AND date_ms < {1:d} ORDER BY date_ms DESC LIMIT 1".format(market, end_ms))
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

    if len(trades) == 0:
        print("empty id: "+str(id_col))
        return

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

    prepend = "p_" + period + "_"

    cur.execute(
        "UPDATE trades SET {0:s}avgPrice = {1:0.8f}, {2:s}highPrice = {3:0.8f}, {4:s}lowPrice = {5:0.8f}, {6:s}startPrice = {7:0.8f}, {8:s}endPrice = {9:0.8f}, {10:s}changeReal = {11:0.8f}, {12:s}changePercent = {13:0.8f}, {14:s}travelReal = {15:0.8f}, {16:s}travelPercent = {17:0.8f}, {18:s}volumePrMin = {19:0.8f}, {20:s}tradesPrMin = {21:0.8f}, {22:s}buysPrMin = {23:0.8f}, {24:s}sellsPrMin = {25:0.8f} WHERE id = {26:d}".format(prepend, avgPrice, prepend, highPrice, prepend, lowPrice, prepend, startPrice, prepend, endPrice, prepend, changeReal, prepend, changePercent, prepend, travelReal, prepend, travelPercent, prepend, volumePrMin, prepend, tradesPrMin, prepend, buysPrMin, prepend, sellsPrMin, id_col))
    conn.commit()

    pastTime = round((end_ms / 1000) - seconds)

    cur.execute(
        "UPDATE trades SET f_{0:s}_changeReal = {1:0.8f}, f_{2:s}_changePercent = {3:0.8f}, f_{4:s}_endPrice = {5:0.8f}, f_{6:s}_lowPrice = {7:0.8f}, f_{8:s}_highPrice = {9:0.8f} WHERE id = {10:d}".format(period, changeReal, period, changePercent, period, endPrice, period, lowPrice, period, highPrice, id_col))
    conn.commit()

while True:
    cur.execute("SELECT id, date_ms FROM trades WHERE market = '" + market + "' AND p_oneTwentyMin_changePercent IS NULL ORDER BY date_ms ASC LIMIT 100")
    rows = cur.fetchall()


    for row in rows:
        print(datetime.fromtimestamp(round(row[1] / 1000)).astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p"))
        getCalcTrades(market, 'fiveSec', row[0], row[1])
        getCalcTrades(market, 'tenSec', row[0], row[1])
        getCalcTrades(market, 'thirtySec', row[0], row[1])
        getCalcTrades(market, 'oneMin', row[0], row[1])
        getCalcTrades(market, 'threeMin', row[0], row[1])
        getCalcTrades(market, 'fiveMin', row[0], row[1])
        getCalcTrades(market, 'tenMin', row[0], row[1])
        getCalcTrades(market, 'fifteenMin', row[0], row[1])
        getCalcTrades(market, 'thirtyMin', row[0], row[1])
        getCalcTrades(market, 'sixtyMin', row[0], row[1])
        getCalcTrades(market, 'oneTwentyMin', row[0], row[1])

    print("Cruncher Takin a break.")

    time.sleep(60)




