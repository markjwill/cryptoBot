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
import numpy as np

import categories
import credentials

market = 'BTCUSDT'

date_ms = None
if len(sys.argv) > 1:
    date_ms = sys.argv[1]

dateExcludes = 'AND ( ( date > 1616282088 ) OR ( date < 1616243798 AND date > 1616004584 ) )'

breakSeconds = 300

blockSeconds = 7200

try:
    conn = mariadb.connect(
        user=credentials.dbUser,
        password=credentials.dbPassword,
        host=credentials.dbHost,
        port=credentials.dbPort,
        database=credentials.dbName
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()

cur.execute("SELECT trades.date FROM trades ORDER BY date ASC")

trades = cur.fetchall()

lastDate = 1615997080

breaks = []

# AND (( date > 1616004584 AND  date < 1616073970 ) OR ( date > 1616088770 AND  date < 1616240913 ) OR ( date > 1616257108 AND  date < 1616243798 ) OR ( date > 1616282088 AND  date < 1616268855 ) OR ( date > 1616284889 AND  date < 1616281056 ) OR ( date > 1616296324 AND  date < 1616282393 ) OR ( date > 1616297171 AND  date < 1616284491 ) OR ( date > 1616299532 AND  date < 1616386037 ) OR ( date > 1616401144 AND  date < 1616600685 ) OR ( date > 1616615807 AND  date < 1616607621 ) OR ( date > 1616625310 ))

for trade in trades:
    # print(datetime.fromtimestamp(lastDate).astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p"))
    if trade[0] - 300 > lastDate:
        #GAP
        breaks.append({
            'before': lastDate - blockSeconds,
            'after': trade[0] + blockSeconds
        })
        print(" GAP min: {0:d}".format(round((trade[0] - lastDate) / 60)))
        print(" FROM ")
        print(datetime.fromtimestamp(lastDate - blockSeconds).astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p"))
        print(datetime.fromtimestamp(lastDate).astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p"))
        print(" TO ")
        print(datetime.fromtimestamp(trade[0] + blockSeconds).astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p"))
        print(datetime.fromtimestamp(trade[0]).astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p"))
    
    lastDate = trade[0]   

string = 'AND (('
first = True
for item in breaks:
    if not first:
        string += ' AND '
    string += ' date < {0:d} ) OR ( date > {1:d}'.format(item['before'], item['after'])
    first = False
string += ' ))'
print(string)



