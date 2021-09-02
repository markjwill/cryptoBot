#!/usr/bin/python

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


def inTargetTime(trade, start_ms, end_ms):
    if trade[3] > start_ms and trade[3] < end_ms:
        return True
    return False


def getRawTrades(market, trades, endSeconds):
    end_ms = endSeconds * 1000
    start_ms = end_ms - 7200000
    query_end_ms = end_ms
    query_start_ms = start_ms
    queryFirst = True
    if len(trades) > 0:
        if len(trades[0]) > 3 and len(trades[-1]) > 3:
            firstTrade_ms = trades[0][3]
            lastTrade_ms = trades[-1][3]
#            print('first trade ms: {0:d} last trade ms: {1:d} diff: {2:d}'.format(firstTrade_ms, lastTrade_ms, (lastTrade_ms - firstTrade_ms)), flush=True)
            if lastTrade_ms < end_ms:
                # just get the newest ones
                query_start_ms = lastTrade_ms
                queryFirst = False
 #               print('last trade ms to end ms', flush=True)
            elif firstTrade_ms > start_ms:
                # just get the oldest ones
                query_end_ms = firstTrade_ms
                newFirst = True
  #              print('start ms to first trade ms', flush=True)

    cur.execute("SELECT price, amount, type, date_ms FROM trades WHERE date_ms > {0:d} AND date_ms < {1:d} ORDER BY date_ms ASC".format(query_start_ms, query_end_ms))
    queryTrades = cur.fetchall()
    # print(str(len(queryTrades))+" trades found", flush=True)
    # print(str(len(trades))+" old trades found", flush=True)

    newTrades = []

    if queryFirst:
        for trade in queryTrades:
            newTrades.append(trade)

    for trade in trades:
        if inTargetTime(trade, start_ms, end_ms):
            newTrades.append(trade)
    
    if not queryFirst:
        for trade in queryTrades:
            newTrades.append(trade)

    # print(str(len(newTrades))+" total trades keeping", flush=True)
    # print('NEW first trade ms: {0:d} last trade ms: {1:d} diff: {2:d}'.format(newTrades[0][3], newTrades[-1][3], (newTrades[-1][3] - newTrades[0][3])), flush=True)
    return newTrades

def getCalcAllPastTrades(market, id_col, trades, endSeconds):
    data = [
        {
            'period' : 'fiveSec',
            'seconds': 5,
            'avgPrice' : 0.0, #avg Price
            'highPrice' : 0.0, #high price
            'lowPrice' : 0.0, #low price
            'startPrice' : 0.0, #start price
            'endPrice' : 0.0, #end price, Current Price
            'changeReal' : 0.0, #start to end change price
            'changePercent' : 0.0, #start to end change price percent
            'travelReal' : 0.0, #max travel in price
            'travelPercent' : 0.0, #max travel in price percent
            'volume' : 0.0, #total volume
            'volumePrMin' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMin' : 0.0,
            'sells' : 0,
            'buysPrMin' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'tenSec',
            'seconds': 10,
            'avgPrice' : 0.0, #avg Price
            'highPrice' : 0.0, #high price
            'lowPrice' : 0.0, #low price
            'startPrice' : 0.0, #start price
            'endPrice' : 0.0, #end price, Current Price
            'changeReal' : 0.0, #start to end change price
            'changePercent' : 0.0, #start to end change price percent
            'travelReal' : 0.0, #max travel in price
            'travelPercent' : 0.0, #max travel in price percent
            'volume' : 0.0, #total volume
            'volumePrMin' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMin' : 0.0,
            'sells' : 0,
            'buysPrMin' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'thirtySec',
            'seconds': 30,
            'avgPrice' : 0.0, #avg Price
            'highPrice' : 0.0, #high price
            'lowPrice' : 0.0, #low price
            'startPrice' : 0.0, #start price
            'endPrice' : 0.0, #end price, Current Price
            'changeReal' : 0.0, #start to end change price
            'changePercent' : 0.0, #start to end change price percent
            'travelReal' : 0.0, #max travel in price
            'travelPercent' : 0.0, #max travel in price percent
            'volume' : 0.0, #total volume
            'volumePrMin' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMin' : 0.0,
            'sells' : 0,
            'buysPrMin' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'oneMin',
            'seconds': 60,
            'avgPrice' : 0.0, #avg Price
            'highPrice' : 0.0, #high price
            'lowPrice' : 0.0, #low price
            'startPrice' : 0.0, #start price
            'endPrice' : 0.0, #end price, Current Price
            'changeReal' : 0.0, #start to end change price
            'changePercent' : 0.0, #start to end change price percent
            'travelReal' : 0.0, #max travel in price
            'travelPercent' : 0.0, #max travel in price percent
            'volume' : 0.0, #total volume
            'volumePrMin' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMin' : 0.0,
            'sells' : 0,
            'buysPrMin' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'threeMin',
            'seconds': 180,
            'avgPrice' : 0.0, #avg Price
            'highPrice' : 0.0, #high price
            'lowPrice' : 0.0, #low price
            'startPrice' : 0.0, #start price
            'endPrice' : 0.0, #end price, Current Price
            'changeReal' : 0.0, #start to end change price
            'changePercent' : 0.0, #start to end change price percent
            'travelReal' : 0.0, #max travel in price
            'travelPercent' : 0.0, #max travel in price percent
            'volume' : 0.0, #total volume
            'volumePrMin' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMin' : 0.0,
            'sells' : 0,
            'buysPrMin' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'fiveMin',
            'seconds': 300,
            'avgPrice' : 0.0, #avg Price
            'highPrice' : 0.0, #high price
            'lowPrice' : 0.0, #low price
            'startPrice' : 0.0, #start price
            'endPrice' : 0.0, #end price, Current Price
            'changeReal' : 0.0, #start to end change price
            'changePercent' : 0.0, #start to end change price percent
            'travelReal' : 0.0, #max travel in price
            'travelPercent' : 0.0, #max travel in price percent
            'volume' : 0.0, #total volume
            'volumePrMin' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMin' : 0.0,
            'sells' : 0,
            'buysPrMin' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'tenMin',
            'seconds': 600,
            'avgPrice' : 0.0, #avg Price
            'highPrice' : 0.0, #high price
            'lowPrice' : 0.0, #low price
            'startPrice' : 0.0, #start price
            'endPrice' : 0.0, #end price, Current Price
            'changeReal' : 0.0, #start to end change price
            'changePercent' : 0.0, #start to end change price percent
            'travelReal' : 0.0, #max travel in price
            'travelPercent' : 0.0, #max travel in price percent
            'volume' : 0.0, #total volume
            'volumePrMin' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMin' : 0.0,
            'sells' : 0,
            'buysPrMin' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'fifteenMin',
            'seconds': 900,
            'avgPrice' : 0.0, #avg Price
            'highPrice' : 0.0, #high price
            'lowPrice' : 0.0, #low price
            'startPrice' : 0.0, #start price
            'endPrice' : 0.0, #end price, Current Price
            'changeReal' : 0.0, #start to end change price
            'changePercent' : 0.0, #start to end change price percent
            'travelReal' : 0.0, #max travel in price
            'travelPercent' : 0.0, #max travel in price percent
            'volume' : 0.0, #total volume
            'volumePrMin' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMin' : 0.0,
            'sells' : 0,
            'buysPrMin' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'thirtyMin',
            'seconds': 1800,
            'avgPrice' : 0.0, #avg Price
            'highPrice' : 0.0, #high price
            'lowPrice' : 0.0, #low price
            'startPrice' : 0.0, #start price
            'endPrice' : 0.0, #end price, Current Price
            'changeReal' : 0.0, #start to end change price
            'changePercent' : 0.0, #start to end change price percent
            'travelReal' : 0.0, #max travel in price
            'travelPercent' : 0.0, #max travel in price percent
            'volume' : 0.0, #total volume
            'volumePrMin' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMin' : 0.0,
            'sells' : 0,
            'buysPrMin' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'sixtyMin',
            'seconds': 3600,
            'avgPrice' : 0.0, #avg Price
            'highPrice' : 0.0, #high price
            'lowPrice' : 0.0, #low price
            'startPrice' : 0.0, #start price
            'endPrice' : 0.0, #end price, Current Price
            'changeReal' : 0.0, #start to end change price
            'changePercent' : 0.0, #start to end change price percent
            'travelReal' : 0.0, #max travel in price
            'travelPercent' : 0.0, #max travel in price percent
            'volume' : 0.0, #total volume
            'volumePrMin' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMin' : 0.0,
            'sells' : 0,
            'buysPrMin' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'oneTwentyMin',
            'seconds': 7200,
            'avgPrice' : 0.0, #avg Price
            'highPrice' : 0.0, #high price
            'lowPrice' : 0.0, #low price
            'startPrice' : 0.0, #start price
            'endPrice' : 0.0, #end price, Current Price
            'changeReal' : 0.0, #start to end change price
            'changePercent' : 0.0, #start to end change price percent
            'travelReal' : 0.0, #max travel in price
            'travelPercent' : 0.0, #max travel in price percent
            'volume' : 0.0, #total volume
            'volumePrMin' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMin' : 0.0,
            'sells' : 0,
            'buysPrMin' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
    ]

    trades = getRawTrades(market, trades, endSeconds)

    if len(trades) == 0:
        print("empty id: "+str(id_col))
        return

    end_ms = endSeconds * 1000

    for trade in trades:
        for period in data:
            period['start_ms'] = end_ms - ( period['seconds'] * 1000 )
            if trade[3] >= period['start_ms'] and trade[3] <= end_ms:
                if period['endPrice'] == 0.0:
                    period['endPrice'] = trade[0]
                period['startPrice'] = trade[0]
                period['tradeCount'] += 1
                if trade[0] > period['highPrice']:
                    period['highPrice'] = trade[0]

                if trade[0] < period['lowPrice'] or period['lowPrice'] == 0.0:
                    period['lowPrice'] = trade[0]

                period['totalPrice'] += trade[0]
                period['volume'] += trade[1]

                if trade[2] == 'buy':
                    period['buys'] += 1

                if trade[2] == 'sell':
                    period['sells'] += 1

    for period in data:
        if period['startPrice'] == 0.0:
            period['startPrice'] = trades[0][0]
            period['endPrice'] = trades[0][0]
            period['highPrice'] = trades[0][0]
            period['lowPrice'] = trades[0][0]
        period['avgPrice'] = trades[0][0] if (period['tradeCount'] == 0) else (period['totalPrice'] / period['tradeCount'])
        period['volumePrMin'] = period['volume'] / period['seconds'] * 60
        period['changeReal'] = period['endPrice'] - period['startPrice']
        period['changePercent'] = 0 if (period['startPrice'] == 0) else (period['changeReal'] * 100 / period['startPrice'])

        period['travelReal'] = period['highPrice'] - period['lowPrice']
        period['travelPercent'] = period['travelReal'] * 100 / period['lowPrice']

        period['tradesPrMin'] = period['tradeCount'] / period['seconds'] * 60
        period['buysPrMin'] = period['buys'] / period['seconds'] * 60
        period['sellsPrMin'] = period['sells'] / period['seconds'] * 60

        prepend = "p_" + period['period'] + "_"

    cur.execute(
            'UPDATE trades SET p_fiveSec_avgPrice = {0:0.8f}, p_fiveSec_highPrice = {1:0.8f}, p_fiveSec_lowPrice = {2:0.8f}, p_fiveSec_startPrice = {3:0.8f}, p_fiveSec_endPrice = {4:0.8f}, p_fiveSec_changeReal = {5:0.8f}, p_fiveSec_changePercent = {6:0.8f}, p_fiveSec_travelReal = {7:0.8f}, p_fiveSec_travelPercent = {8:0.8f}, p_fiveSec_volumePrMin = {9:0.8f}, p_fiveSec_tradesPrMin = {10:0.8f}, p_fiveSec_buysPrMin = {11:0.8f}, p_fiveSec_sellsPrMin = {12:0.8f}, p_tenSec_avgPrice = {13:0.8f}, p_tenSec_highPrice = {14:0.8f}, p_tenSec_lowPrice = {15:0.8f}, p_tenSec_startPrice = {16:0.8f}, p_tenSec_endPrice = {17:0.8f}, p_tenSec_changeReal = {18:0.8f}, p_tenSec_changePercent = {19:0.8f}, p_tenSec_travelReal = {20:0.8f}, p_tenSec_travelPercent = {21:0.8f}, p_tenSec_volumePrMin = {22:0.8f}, p_tenSec_tradesPrMin = {23:0.8f}, p_tenSec_buysPrMin = {24:0.8f}, p_tenSec_sellsPrMin = {25:0.8f}, p_thirtySec_avgPrice = {26:0.8f}, p_thirtySec_highPrice = {27:0.8f}, p_thirtySec_lowPrice = {28:0.8f}, p_thirtySec_startPrice = {29:0.8f}, p_thirtySec_endPrice = {30:0.8f}, p_thirtySec_changeReal = {31:0.8f}, p_thirtySec_changePercent = {32:0.8f}, p_thirtySec_travelReal = {33:0.8f}, p_thirtySec_travelPercent = {34:0.8f}, p_thirtySec_volumePrMin = {35:0.8f}, p_thirtySec_tradesPrMin = {36:0.8f}, p_thirtySec_buysPrMin = {37:0.8f}, p_thirtySec_sellsPrMin = {38:0.8f}, p_oneMin_avgPrice = {39:0.8f}, p_oneMin_highPrice = {40:0.8f}, p_oneMin_lowPrice = {41:0.8f}, p_oneMin_startPrice = {42:0.8f}, p_oneMin_endPrice = {43:0.8f}, p_oneMin_changeReal = {44:0.8f}, p_oneMin_changePercent = {45:0.8f}, p_oneMin_travelReal = {46:0.8f}, p_oneMin_travelPercent = {47:0.8f}, p_oneMin_volumePrMin = {48:0.8f}, p_oneMin_tradesPrMin = {49:0.8f}, p_oneMin_buysPrMin = {50:0.8f}, p_oneMin_sellsPrMin = {51:0.8f}, p_threeMin_avgPrice = {52:0.8f}, p_threeMin_highPrice = {53:0.8f}, p_threeMin_lowPrice = {54:0.8f}, p_threeMin_startPrice = {55:0.8f}, p_threeMin_endPrice = {56:0.8f}, p_threeMin_changeReal = {57:0.8f}, p_threeMin_changePercent = {58:0.8f}, p_threeMin_travelReal = {59:0.8f}, p_threeMin_travelPercent = {60:0.8f}, p_threeMin_volumePrMin = {61:0.8f}, p_threeMin_tradesPrMin = {62:0.8f}, p_threeMin_buysPrMin = {63:0.8f}, p_threeMin_sellsPrMin = {64:0.8f}, p_fiveMin_avgPrice = {65:0.8f}, p_fiveMin_highPrice = {66:0.8f}, p_fiveMin_lowPrice = {67:0.8f}, p_fiveMin_startPrice = {68:0.8f}, p_fiveMin_endPrice = {69:0.8f}, p_fiveMin_changeReal = {70:0.8f}, p_fiveMin_changePercent = {71:0.8f}, p_fiveMin_travelReal = {72:0.8f}, p_fiveMin_travelPercent = {73:0.8f}, p_fiveMin_volumePrMin = {74:0.8f}, p_fiveMin_tradesPrMin = {75:0.8f}, p_fiveMin_buysPrMin = {76:0.8f}, p_fiveMin_sellsPrMin = {77:0.8f}, p_tenMin_avgPrice = {78:0.8f}, p_tenMin_highPrice = {79:0.8f}, p_tenMin_lowPrice = {80:0.8f}, p_tenMin_startPrice = {81:0.8f}, p_tenMin_endPrice = {82:0.8f}, p_tenMin_changeReal = {83:0.8f}, p_tenMin_changePercent = {84:0.8f}, p_tenMin_travelReal = {85:0.8f}, p_tenMin_travelPercent = {86:0.8f}, p_tenMin_volumePrMin = {87:0.8f}, p_tenMin_tradesPrMin = {88:0.8f}, p_tenMin_buysPrMin = {89:0.8f}, p_tenMin_sellsPrMin = {90:0.8f}, p_fifteenMin_avgPrice = {91:0.8f}, p_fifteenMin_highPrice = {92:0.8f}, p_fifteenMin_lowPrice = {93:0.8f}, p_fifteenMin_startPrice = {94:0.8f}, p_fifteenMin_endPrice = {95:0.8f}, p_fifteenMin_changeReal = {96:0.8f}, p_fifteenMin_changePercent = {97:0.8f}, p_fifteenMin_travelReal = {98:0.8f}, p_fifteenMin_travelPercent = {99:0.8f}, p_fifteenMin_volumePrMin = {100:0.8f}, p_fifteenMin_tradesPrMin = {101:0.8f}, p_fifteenMin_buysPrMin = {102:0.8f}, p_fifteenMin_sellsPrMin = {103:0.8f}, p_thirtyMin_avgPrice = {104:0.8f}, p_thirtyMin_highPrice = {105:0.8f}, p_thirtyMin_lowPrice = {106:0.8f}, p_thirtyMin_startPrice = {107:0.8f}, p_thirtyMin_endPrice = {108:0.8f}, p_thirtyMin_changeReal = {109:0.8f}, p_thirtyMin_changePercent = {110:0.8f}, p_thirtyMin_travelReal = {111:0.8f}, p_thirtyMin_travelPercent = {112:0.8f}, p_thirtyMin_volumePrMin = {113:0.8f}, p_thirtyMin_tradesPrMin = {114:0.8f}, p_thirtyMin_buysPrMin = {115:0.8f}, p_thirtyMin_sellsPrMin = {116:0.8f}, p_sixtyMin_avgPrice = {117:0.8f}, p_sixtyMin_highPrice = {118:0.8f}, p_sixtyMin_lowPrice = {119:0.8f}, p_sixtyMin_startPrice = {120:0.8f}, p_sixtyMin_endPrice = {121:0.8f}, p_sixtyMin_changeReal = {122:0.8f}, p_sixtyMin_changePercent = {123:0.8f}, p_sixtyMin_travelReal = {124:0.8f}, p_sixtyMin_travelPercent = {125:0.8f}, p_sixtyMin_volumePrMin = {126:0.8f}, p_sixtyMin_tradesPrMin = {127:0.8f}, p_sixtyMin_buysPrMin = {128:0.8f}, p_sixtyMin_sellsPrMin = {129:0.8f}, p_oneTwentyMin_avgPrice = {130:0.8f}, p_oneTwentyMin_highPrice = {131:0.8f}, p_oneTwentyMin_lowPrice = {132:0.8f}, p_oneTwentyMin_startPrice = {133:0.8f}, p_oneTwentyMin_endPrice = {134:0.8f}, p_oneTwentyMin_changeReal = {135:0.8f}, p_oneTwentyMin_changePercent = {136:0.8f}, p_oneTwentyMin_travelReal = {137:0.8f}, p_oneTwentyMin_travelPercent = {138:0.8f}, p_oneTwentyMin_volumePrMin = {139:0.8f}, p_oneTwentyMin_tradesPrMin = {140:0.8f}, p_oneTwentyMin_buysPrMin = {141:0.8f}, p_oneTwentyMin_sellsPrMin = {142:0.8f} WHERE id = {143:d}'.format(data[0]['avgPrice'],data[0]['highPrice'],data[0]['lowPrice'],data[0]['startPrice'],data[0]['endPrice'],data[0]['changeReal'],data[0]['changePercent'],data[0]['travelReal'],data[0]['travelPercent'],data[0]['volumePrMin'],data[0]['tradesPrMin'],data[0]['buysPrMin'],data[0]['sellsPrMin'],data[1]['avgPrice'],data[1]['highPrice'],data[1]['lowPrice'],data[1]['startPrice'],data[1]['endPrice'],data[1]['changeReal'],data[1]['changePercent'],data[1]['travelReal'],data[1]['travelPercent'],data[1]['volumePrMin'],data[1]['tradesPrMin'],data[1]['buysPrMin'],data[1]['sellsPrMin'],data[2]['avgPrice'],data[2]['highPrice'],data[2]['lowPrice'],data[2]['startPrice'],data[2]['endPrice'],data[2]['changeReal'],data[2]['changePercent'],data[2]['travelReal'],data[2]['travelPercent'],data[2]['volumePrMin'],data[2]['tradesPrMin'],data[2]['buysPrMin'],data[2]['sellsPrMin'],data[3]['avgPrice'],data[3]['highPrice'],data[3]['lowPrice'],data[3]['startPrice'],data[3]['endPrice'],data[3]['changeReal'],data[3]['changePercent'],data[3]['travelReal'],data[3]['travelPercent'],data[3]['volumePrMin'],data[3]['tradesPrMin'],data[3]['buysPrMin'],data[3]['sellsPrMin'],data[4]['avgPrice'],data[4]['highPrice'],data[4]['lowPrice'],data[4]['startPrice'],data[4]['endPrice'],data[4]['changeReal'],data[4]['changePercent'],data[4]['travelReal'],data[4]['travelPercent'],data[4]['volumePrMin'],data[4]['tradesPrMin'],data[4]['buysPrMin'],data[4]['sellsPrMin'],data[5]['avgPrice'],data[5]['highPrice'],data[5]['lowPrice'],data[5]['startPrice'],data[5]['endPrice'],data[5]['changeReal'],data[5]['changePercent'],data[5]['travelReal'],data[5]['travelPercent'],data[5]['volumePrMin'],data[5]['tradesPrMin'],data[5]['buysPrMin'],data[5]['sellsPrMin'],data[6]['avgPrice'],data[6]['highPrice'],data[6]['lowPrice'],data[6]['startPrice'],data[6]['endPrice'],data[6]['changeReal'],data[6]['changePercent'],data[6]['travelReal'],data[6]['travelPercent'],data[6]['volumePrMin'],data[6]['tradesPrMin'],data[6]['buysPrMin'],data[6]['sellsPrMin'],data[7]['avgPrice'],data[7]['highPrice'],data[7]['lowPrice'],data[7]['startPrice'],data[7]['endPrice'],data[7]['changeReal'],data[7]['changePercent'],data[7]['travelReal'],data[7]['travelPercent'],data[7]['volumePrMin'],data[7]['tradesPrMin'],data[7]['buysPrMin'],data[7]['sellsPrMin'],data[8]['avgPrice'],data[8]['highPrice'],data[8]['lowPrice'],data[8]['startPrice'],data[8]['endPrice'],data[8]['changeReal'],data[8]['changePercent'],data[8]['travelReal'],data[8]['travelPercent'],data[8]['volumePrMin'],data[8]['tradesPrMin'],data[8]['buysPrMin'],data[8]['sellsPrMin'],data[9]['avgPrice'],data[9]['highPrice'],data[9]['lowPrice'],data[9]['startPrice'],data[9]['endPrice'],data[9]['changeReal'],data[9]['changePercent'],data[9]['travelReal'],data[9]['travelPercent'],data[9]['volumePrMin'],data[9]['tradesPrMin'],data[9]['buysPrMin'],data[9]['sellsPrMin'],data[10]['avgPrice'],data[10]['highPrice'],data[10]['lowPrice'],data[10]['startPrice'],data[10]['endPrice'],data[10]['changeReal'],data[10]['changePercent'],data[10]['travelReal'],data[10]['travelPercent'],data[10]['volumePrMin'],data[10]['tradesPrMin'],data[10]['buysPrMin'],data[10]['sellsPrMin'],id_col))
    conn.commit()

    return trades, data[2]['avgPrice'], data[5]['avgPrice'], data[8]['avgPrice'], data[10]['avgPrice']

def getCalcFutureTrades(market, period, id_col, end_ms):
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

def getLatestNumbers(trades):
    cur.execute("SELECT id, date_ms FROM trades ORDER BY date_ms DESC LIMIT 1")
    rows = cur.fetchall()

    oldTrades = trades
    trades = []

    for row in rows:
        print(datetime.fromtimestamp(round(row[1] / 1000)).astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p"))
        trades, tSecAvg, fiveAvg, thirtyAvg, oneTwentyAvg = getCalcAllPastTrades(market, row[0], oldTrades, round(row[1] / 1000))

    return trades, tSecAvg, fiveAvg, thirtyAvg, oneTwentyAvg

def crunchPastNumbers(trades):
    cur.execute("SELECT id, date_ms FROM trades WHERE p_oneTwentyMin_changePercent IS NULL ORDER BY date_ms DESC LIMIT 100")
    rows = cur.fetchall()

    for row in rows:
        print(datetime.fromtimestamp(round(row[1] / 1000)).astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p"))
        trades, tSecAvg, fiveAvg, thirtyAvg, oneTwentyAvg = getCalcAllPastTrades(market, row[0], oldTrades, round(row[1] / 1000))

    return

def crunchFutureNumbers(trades):
    twoHrsAgo = updatedTime.timestamp() * 1000
    cur.execute("SELECT id, date_ms FROM trades WHERE p_oneTwentyMin_changePercent IS NOT NULL AND f_oneTwentyMin_changePercent IS NULL AND date_ms < {0:} ORDER BY date_ms DESC LIMIT 100".format(twoHrsAgo))
    for row in rows:
        print("FutureCruncher "+datetime.fromtimestamp(math.floor(row[1] / 1000)).astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p")+" "+str(math.floor(row[1] / 1000)), flush=True)
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
    return trades



