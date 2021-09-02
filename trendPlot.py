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

date_ms = 1614664800000
if len(sys.argv) > 1:
    date_ms = int(sys.argv[1])

limit = 500

date = round(date_ms / 1000)
start = date - round( 0.5 * limit)
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


cur.execute("SELECT date, price, p_fiveMin_avgPrice, p_thirtyMin_avgPrice, p_oneTwentyMin_avgPrice, p_oneMin_avgPrice FROM trades WHERE market = '{0:s}' AND p_thirtyMin_avgPrice IS NOT NULL AND date > {1:d} ORDER BY date_ms ASC LIMIT {2:d}".format(market, start, limit))

dates = []
price = []
fiveMinPrice = []
thirtyMinPrice = []
oneTwentyMinPrice = []
oneMinPrice = []

trades = cur.fetchall()

for trade in trades:
    price.append(trade[1])
    oneMinPrice.append(trade[5])
    fiveMinPrice.append(trade[2])
    thirtyMinPrice.append(trade[3])
    oneTwentyMinPrice.append(trade[4])
    if trade[0] == date:
        print("price: {0:f} 5: {1:f} 30:{2:f} 120:{3:f} 10s:{4:f}".format(trade[1], trade[2], trade[3], trade[4], trade[5]))


plt.plot(oneTwentyMinPrice, label="avg 120 Price")
plt.plot(thirtyMinPrice, label="avg 30 Price")
plt.plot(fiveMinPrice, label="avg 5 Price")
# plt.plot(oneMinPrice, label="avg 1 Price")
plt.plot(price, label="price")

plt.grid(True)

plt.show()
