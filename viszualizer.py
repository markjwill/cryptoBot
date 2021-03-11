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

startTime = 1614723828
endTime = 1614725028

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

cur.execute("SELECT p_oneTwentyMin_changePercent,p_sixtyMin_changePercent,p_thirtyMin_changePercent,p_fifteenMin_changePercent,p_tenMin_changePercent,p_fiveMin_changePercent,p_threeMin_changePercent,p_oneMin_changePercent,p_thirtySec_changePercent,p_tenSec_changePercent,p_fiveSec_changePercent,price,f_fiveSec_changePercent,f_tenSec_changePercent,f_thirtySec_changePercent,f_oneMin_changePercent,f_threeMin_changePercent,f_fiveMin_changePercent,f_tenMin_changePercent,f_fifteenMin_changePercent,f_thirtyMin_changePercent,f_sixtyMin_changePercent,f_oneTwentyMin_changePercent FROM trades WHERE market = '{0:s}' AND date > {1:d} AND date < {2:d} ORDER BY date_ms DESC LIMIT 0,50000".format(market, startTime, endTime))



trades = cur.fetchall()


# 0  p_oneTwentyMin_changePercent,
# 1  p_sixtyMin_changePercent,
# 2  p_thirtyMin_changePercent,
# 3  p_fifteenMin_changePercent,
# 4  p_tenMin_changePercent,
# 5  p_fiveMin_changePercent,
# 6  p_threeMin_changePercent,
# 7  p_oneMin_changePercent,
# 8  p_thirtySec_changePercent,
# 9  p_tenSec_changePercent,
# 10 p_fiveSec_changePercent,
# 11 price,
# 12 f_fiveSec_changePercent,
# 13 f_tenSec_changePercent,
# 14 f_thirtySec_changePercent,
# 15 f_oneMin_changePercent,
# 16 f_threeMin_changePercent,
# 17 f_fiveMin_changePercent,
# 18 f_tenMin_changePercent,
# 19 f_fifteenMin_changePercent,
# 20 f_thirtyMin_changePercent,
# 21 f_sixtyMin_changePercent,
# 22 f_oneTwentyMin_changePercent

labels = {
    0  : "p_oneTwentyMin",
    1  : "p_sixtyMin",
    2  : "p_thirtyMin",
    3  : "p_fifteenMin",
    4  : "p_tenMin",
    5  : "p_fiveMin",
    6  : "p_threeMin",
    7  : "p_oneMin",
    8  : "p_thirtySec",
    9  : "p_tenSec",
    10 : "p_fiveSec",
    11 : "price",
    12 : "f_fiveSec",
    13 : "f_tenSec",
    14 : "f_thirtySec",
    15 : "f_oneMin",
    16 : "f_threeMin",
    17 : "f_fiveMin",
    18 : "f_tenMin",
    19 : "f_fifteenMin",
    20 : "f_thirtyMin",
    21 : "f_sixtyMin",
    22 : "f_oneTwentyMin"
}

strings = {
    0  : "",
    1  : "",
    2  : "",
    3  : "",
    4  : "",
    5  : "",
    6  : "",
    7  : "",
    8  : "",
    9  : "",
    10 : "",
    11 : "",
    12 : "",
    13 : "",
    14 : "",
    15 : "",
    16 : "",
    17 : "",
    18 : "",
    19 : "",
    20 : "",
    21 : "",
    22 : "n"
}

mins = {
    0  : 0.0,
    1  : 0.0,
    2  : 0.0,
    3  : 0.0,
    4  : 0.0,
    5  : 0.0,
    6  : 0.0,
    7  : 0.0,
    8  : 0.0,
    9  : 0.0,
    10 : 0.0,
    11 : 0.0,
    12 : 0.0,
    13 : 0.0,
    14 : 0.0,
    15 : 0.0,
    16 : 0.0,
    17 : 0.0,
    18 : 0.0,
    19 : 0.0,
    20 : 0.0,
    21 : 0.0,
    22 : 0.0
}

maxs = {
    0  : 0.0,
    1  : 0.0,
    2  : 0.0,
    3  : 0.0,
    4  : 0.0,
    5  : 0.0,
    6  : 0.0,
    7  : 0.0,
    8  : 0.0,
    9  : 0.0,
    10 : 0.0,
    11 : 0.0,
    12 : 0.0,
    13 : 0.0,
    14 : 0.0,
    15 : 0.0,
    16 : 0.0,
    17 : 0.0,
    18 : 0.0,
    19 : 0.0,
    20 : 0.0,
    21 : 0.0,
    22 : 0.0
}
floats = {}

print("{0:15s}{1:15s}{2:15s}{3:15s}{4:15s}{5:15s}{6:15s}{7:15s}{8:15s}{9:15s}{10:15s}{11:15s}{12:15s}{13:15s}{14:15s}{15:15s}{16:15s}{17:15s}{18:15s}{19:15s}{20:15s}{21:15s}{22:15s}".format(labels[0],labels[1],labels[2],labels[3],labels[4],labels[5],labels[6],labels[7],labels[8],labels[9],labels[10],labels[11],labels[12],labels[13],labels[14],labels[15],labels[16],labels[17],labels[18],labels[19],labels[20],labels[21],labels[22]))

for trade in trades:
    for i, v in enumerate(trade):
        if v is not None:
            if v > maxs[i] or maxs[i] == 0:
                if v > 0:
                    maxs[i] = v
            if v < mins[i] or mins[i] == 0:
                if v < 0:
                    mins[i] = v

for trade in trades:
    floats = {}
    for i, v in enumerate(trade):
        x = v
        if v is None:
            x = 0.0
        if x > (maxs[i] * 0.66):
            strings[i] = col.end+col.maxg
        if x > (maxs[i] * 0.33):
            strings[i] = col.end+col.midg
        elif x > 0:
            strings[i] = col.end+col.ming
        elif x == 0.0:
            strings[i] = col.end
        elif x < (mins[i] * 0.5):
            strings[i] = col.end+col.maxr
        elif x < (mins[i] * 0.25):
            strings[i] = col.end+col.midr
        else:
            strings[i] = col.end+col.minr
        floats[i] = x

    print("{0:s}{1:15.6f}{2:s}{3:15.6f}{4:s}{5:15.6f}{6:s}{7:15.6f}{8:s}{9:15.6f}{10:s}{11:15.6f}{12:s}{13:15.6f}{14:s}{15:15.6f}{16:s}{17:15.6f}{18:s}{19:15.6f}{20:s}{21:15.6f}{22:s}{23:15.6f}{24:s}{25:15.6f}{26:s}{27:15.6f}{28:s}{29:15.6f}{30:s}{31:15.6f}{32:s}{33:15.6f}{34:s}{35:15.6f}{36:s}{37:15.6f}{38:s}{39:15.6f}{40:s}{41:15.6f}{42:s}{43:15.6f}{44:s}{45:15.6f}".format(strings[0],floats[0],strings[1],floats[1],strings[2],floats[2],strings[3],floats[3],strings[4],floats[4],strings[5],floats[5],strings[6],floats[6],strings[7],floats[7],strings[8],floats[8],strings[9],floats[9],strings[10],floats[10],strings[11],floats[11],strings[12],floats[12],strings[13],floats[13],strings[14],floats[14],strings[15],floats[15],strings[16],floats[16],strings[17],floats[17],strings[18],floats[18],strings[19],floats[19],strings[20],floats[20],strings[21],floats[21],strings[22],floats[22]))
print(col.end)
print(mins)
print(maxs)
