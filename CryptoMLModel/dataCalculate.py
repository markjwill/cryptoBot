from __future__ import unicode_literals
import time
from datetime import datetime, timedelta
import ssl
import sys
import mariadb
import pprint
from pytz import timezone
import math
from timeit import default_timer as timer
import mydb
import pandas as pd

# trades[0] price
# trades[1] amount
# trades[2] type
# trades[3] date_ms

# trades must be in Ascending order

#  Add Column
# df.insert(loc, column, value)

#  Add Column with Formula
# df.assign(temp_f=lambda x: x.temp_c * 9 / 5 + 32)

#  Assign 1 value
# df.at['C', 'x'] = 10
# df.at[7, 'Product_Name'] = 'Test Product'

# Insert Dict to the dataframe using DataFrame.append()
# new_row = {'Courses':'Hyperion', 'Fee':24000, 'Duration':'55days', 'Discount':1800}
# df2 = df.append(new_row, ignore_index=True)

TIME_PERIODS = {
    'fiveSeconds': 5,
    'tenSeconds': 10,
    'thirtySeconds': 30,
    'oneMinute': 60,
    'threeMinutes': 180,
    'fiveMinutes': 300,
    'tenMinutes': 600,
    'fifteenMinutes': 900,
    'thirtyMinutes': 1800,
    'sixtyMinutes': 3600,
    'oneTwentyMinutes': 7200
}

PERIOD_FEATURES = {
    'avgPrice' : 0.0, #avg Price
    'highPrice' : 0.0, #high price
    'lowPrice' : 0.0, #low price
    'startPrice' : 0.0, #start price
    'endPrice' : 0.0, #end price
    'changeReal' : 0.0, #start to end change price
    'changePercent' : 0.0, #start to end change price percent
    'travelReal' : 0.0, #low to high change in price
    'travelPercent' : 0.0, #low to high change price percent
    'volume' : 0.0, #sum of trade amounts
    'volumePrMinute' : 0.0, #sum of trade amounts per minute
    'sellsPrMinute' : 0.0, #number of sells per minute
    'sells' : 0, #number of sells
    'buysPrMinute' : 0.0, #number of buys per minute
    'buys' : 0, #number of buys
    'buyVsSell' : 0 #sum of buys as +1 and sells as -1
    'buyVsSellVolume' : 0 #sum of buys as +volume and sells as -volume
    'tradeCount' : 0 #number of trades
}

def noDataGaps(trades):
    lastTimeMilliSeconds = trades[0][3]
    for trade in trades:
        if trade[3] - 300 > lastTimeMilliSeconds:
            return False
        lastTimeMilliSeconds = trade[3]
    return True

def selectTrades(tradePool, startTimeMiliSeconds, endTimeMilliSeconds):
    trades = []
    if tradePool[0][3] > endTimeMilliSeconds:
        return False
    if tradePool[-1][3] < startTimeMilliSeconds:
        return False
    for trade in tradePool:
        if trade[3] > startTimeMiliSeconds and trade[3] < endTimeMilliSeconds:
            trades.append(trade)
    return trades

def setupDataFrame():
    df = pd.DataFrame()
    columns = []
    for periodName, periodMilliseconds in TIME_PERIODS.items():
        for featureName in PERIOD_FEATURES:
            columns.append(f'past_{periodName}_{featureName}')

    for periodName, periodMilliseconds in TIME_PERIODS.items():
        columns.append(f'future_{periodName}_endPrice')

    df = df.reindex(columns = columns)

    return df

def calculateFeatures(df, trades, tradeTimeMilliSeconds):
    allFeatures= {}

    for name, periodMilliseconds in TIME_PERIODS.items():
        startTimeMiliSeconds = tradeTimeMiliSeconds - PeriodMilliSeconds
        endTimeMiliseconds = tradeTimeMilliSeconds
        periodTrades = selectTrades(trades, startTimeMilliSeconds, endTimeMilliSeconds)
        pastFeatures = calculatePastFeatures(periodTrades, PeriodMilliSeconds)
        pastFeatures = {f'past_{name}_{k}': v for k, v in pastFeatures.items()}
        allFeatures = allFeatures | pastFeatures

        startTimeMiliSeconds = tradeTimeMiliSeconds 
        endTimeMiliseconds = tradeTimeMilliSeconds + PeriodMilliSeconds
        periodTrades = selectTrades(trades, startTimeMilliSeconds, endTimeMilliSeconds)
        futureFeatures = calculateFutureFeatures(periodTrades)
        futureFeatures = {f'future_{name}_{k}': v for k, v in pastFeatures.items()}
        allFeatures = allFeatures | futureFeatures

    df.append(allFeatures)


def calcuateFutureFeatures(trades):
    features = {
        'endPrice' : trades[0][0], #end price
    }
    return features

def calculatePastFeatures(trades, millieSeconds):
    features = PERIOD_FEATURES
    features['startPrice'] = trades[-1][0]
    features['endPrice'] = trades[0][0]

    for trade in trades:
        features['tradeCount'] += 1
        if trade[0] > features['highPrice']:
            features['highPrice'] = trade[0]

        if trade[0] < features['lowPrice'] or features['lowPrice'] == 0.0:
            features['lowPrice'] = trade[0]

        features['volume'] += trade[1]

        if trade[2] == 'buy':
            features['buys'] += 1
            features['buysVsSell'] += 1
            features['buyVsSellVolume'] += trade[1]

        if trade[2] == 'sell':
            features['sells'] += 1
            features['buysVsSell'] -= 1
            features['buyVsSellVolume'] -= trade[1]

    if features['tradeCount'] == 0:
        return False
    if features['lowPrice'] == 0:
        return False

    features['avgPrice'] = features['totalPrice'] / features['tradeCount']
    features['volumePrMinute'] = features['volume'] / millieSeconds * 60 * 1000
    features['changeReal'] = features['endPrice'] - features['startPrice']
    features['changePercent'] = features['changeReal'] * 100 / features['startPrice']
    features['travelReal'] = features['highPrice'] - features['lowPrice']
    features['travelPercent'] = features['travelReal'] * 100 / features['lowPrice']
    features['tradesPrMinute'] = features['tradeCount'] / millieSeconds * 60 * 1000
    features['buysPrMinute'] = features['buys'] / millieSeconds * 60 * 1000
    features['sellsPrMinute'] = features['sells'] / millieSeconds * 60 * 1000

    return features



market = 'BTCUSDT'


def getRawTrades(market, end_ms, seconds = 0):
    pastTime = round(end_ms - (seconds * 1000 ))
    cur.execute("SELECT price, amount, type, date_ms FROM trades WHERE date_ms > {0:d} AND date_ms < {1:d} ORDER BY date_ms DESC".format(pastTime, end_ms))
    trades = cur.fetchall()

    if not len(trades):
        print("No rows found, selecting most recent")
        cur.execute("SELECT price, amount, type, date_ms FROM trades WHERE date_ms < {0:d} ORDER BY date_ms DESC LIMIT 1".format(end_ms))
        trades = cur.fetchall()

    return trades

def getCalcTrades(market, period, id_col, end_ms):
    if period == 'fiveSeconds':
        seconds = 5
    elif period == 'tenSeconds':
        seconds = 10
    elif period == 'thirtySeconds':
        seconds = 30
    elif period == 'oneMinute':
        seconds = 60
    elif period == 'threeMinute':
        seconds = 180
    elif period == 'fiveMinute':
        seconds = 300
    elif period == 'tenMinute':
        seconds = 600
    elif period == 'fifteenMinute':
        seconds = 900
    elif period == 'thirtyMinute':
        seconds = 1800
    elif period == 'sixtyMinute':
        seconds = 3600
    elif period == 'oneTwentyMinute':
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

def getCalcAllTrades(market, id_col, end_ms):
    data = [
        {
            'period' : 'fiveSeconds',
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
            'volumePrMinute' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMinute' : 0.0,
            'sells' : 0,
            'buysPrMinute' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'tenSeconds',
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
            'volumePrMinute' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMinute' : 0.0,
            'sells' : 0,
            'buysPrMinute' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'thirtySeconds',
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
            'volumePrMinute' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMinute' : 0.0,
            'sells' : 0,
            'buysPrMinute' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'oneMinute',
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
            'volumePrMinute' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMinute' : 0.0,
            'sells' : 0,
            'buysPrMinute' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'threeMinute',
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
            'volumePrMinute' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMinute' : 0.0,
            'sells' : 0,
            'buysPrMinute' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'fiveMinute',
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
            'volumePrMinute' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMinute' : 0.0,
            'sells' : 0,
            'buysPrMinute' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'tenMinute',
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
            'volumePrMinute' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMinute' : 0.0,
            'sells' : 0,
            'buysPrMinute' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'fifteenMinute',
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
            'volumePrMinute' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMinute' : 0.0,
            'sells' : 0,
            'buysPrMinute' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'thirtyMinute',
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
            'volumePrMinute' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMinute' : 0.0,
            'sells' : 0,
            'buysPrMinute' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'sixtyMinute',
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
            'volumePrMinute' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMinute' : 0.0,
            'sells' : 0,
            'buysPrMinute' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
        {
            'period' : 'oneTwentyMinute',
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
            'volumePrMinute' : 0.0, #
            'totalPrice' : 0.0, #total
            'sellsPrMinute' : 0.0,
            'sells' : 0,
            'buysPrMinute' : 0.0,
            'buys' : 0,
            'tradeCount' : 0,
            'start_ms' : 0
        },
    ]

    trades = getRawTrades(market, end_ms, 7200)

    if len(trades) == 0:
        print("empty id: "+str(id_col))
        return

    for trade in trades:
        for period in data:
            period['start_ms'] = end_ms - ( period['seconds'] * 1000 )
            if trade[3] >= period['start_ms']:
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
        period['avgPrice'] = 0 if (period['tradeCount'] == 0) else (period['totalPrice'] / period['tradeCount'])
        period['volumePrMinute'] = period['volume'] / period['seconds'] * 60
        period['changeReal'] = period['endPrice'] - period['startPrice']
        period['changePercent'] = 0 if (period['startPrice'] == 0) else (period['changeReal'] * 100 / period['startPrice'])

        period['travelReal'] = period['highPrice'] - period['lowPrice']
        period['travelPercent'] = 0 if (period['lowPrice'] == 0) else ( period['travelReal'] * 100 / period['lowPrice'])


        period['tradesPrMinute'] = period['tradeCount'] / period['seconds'] * 60
        period['buysPrMinute'] = period['buys'] / period['seconds'] * 60
        period['sellsPrMinute'] = period['sells'] / period['seconds'] * 60

        prepend = "p_" + period['period'] + "_"

        # cur.execute(
        #     "UPDATE trades SET {0:s}avgPrice = {1:0.8f}, {2:s}highPrice = {3:0.8f}, {4:s}lowPrice = {5:0.8f}, {6:s}startPrice = {7:0.8f}, {8:s}endPrice = {9:0.8f}, {10:s}changeReal = {11:0.8f}, {12:s}changePercent = {13:0.8f}, {14:s}travelReal = {15:0.8f}, {16:s}travelPercent = {17:0.8f}, {18:s}volumePrMin = {19:0.8f}, {20:s}tradesPrMin = {21:0.8f}, {22:s}buysPrMin = {23:0.8f}, {24:s}sellsPrMin = {25:0.8f} WHERE id = {26:d}".format(prepend, period['avgPrice'], prepend, period['highPrice'], prepend, period['lowPrice'], prepend, period['startPrice'], prepend, period['endPrice'], prepend, period['changeReal'], prepend, period['changePercent'], prepend, period['travelReal'], prepend, period['travelPercent'], prepend, period['volumePrMinute'], prepend, period['tradesPrMinute'], prepend, period['buysPrMinute'], prepend, period['sellsPrMinute'], id_col))
        # conn.commit()

        pastTime = round(period['start_ms'] / 1000)

        cur.execute(
            "UPDATE trades SET f_{0:s}_changeReal = {1:0.8f}, f_{2:s}_changePercent = {3:0.8f}, f_{4:s}_endPrice = {5:0.8f}, f_{6:s}_lowPrice = {7:0.8f}, f_{8:s}_highPrice = {9:0.8f} WHERE date = {10:d}".format(period['period'], period['changeReal'], period['period'], period['changePercent'], period['period'], period['endPrice'], period['period'], period['lowPrice'], period['period'], period['highPrice'], pastTime))
        conn.commit()

    cur.execute(
            'UPDATE trades SET p_fiveSec_avgPrice = {0:0.8f}, p_fiveSec_highPrice = {1:0.8f}, p_fiveSec_lowPrice = {2:0.8f}, p_fiveSec_startPrice = {3:0.8f}, p_fiveSec_endPrice = {4:0.8f}, p_fiveSec_changeReal = {5:0.8f}, p_fiveSec_changePercent = {6:0.8f}, p_fiveSec_travelReal = {7:0.8f}, p_fiveSec_travelPercent = {8:0.8f}, p_fiveSec_volumePrMin = {9:0.8f}, p_fiveSec_tradesPrMin = {10:0.8f}, p_fiveSec_buysPrMin = {11:0.8f}, p_fiveSec_sellsPrMin = {12:0.8f}, p_tenSec_avgPrice = {13:0.8f}, p_tenSec_highPrice = {14:0.8f}, p_tenSec_lowPrice = {15:0.8f}, p_tenSec_startPrice = {16:0.8f}, p_tenSec_endPrice = {17:0.8f}, p_tenSec_changeReal = {18:0.8f}, p_tenSec_changePercent = {19:0.8f}, p_tenSec_travelReal = {20:0.8f}, p_tenSec_travelPercent = {21:0.8f}, p_tenSec_volumePrMin = {22:0.8f}, p_tenSec_tradesPrMin = {23:0.8f}, p_tenSec_buysPrMin = {24:0.8f}, p_tenSec_sellsPrMin = {25:0.8f}, p_thirtySec_avgPrice = {26:0.8f}, p_thirtySec_highPrice = {27:0.8f}, p_thirtySec_lowPrice = {28:0.8f}, p_thirtySec_startPrice = {29:0.8f}, p_thirtySec_endPrice = {30:0.8f}, p_thirtySec_changeReal = {31:0.8f}, p_thirtySec_changePercent = {32:0.8f}, p_thirtySec_travelReal = {33:0.8f}, p_thirtySec_travelPercent = {34:0.8f}, p_thirtySec_volumePrMin = {35:0.8f}, p_thirtySec_tradesPrMin = {36:0.8f}, p_thirtySec_buysPrMin = {37:0.8f}, p_thirtySec_sellsPrMin = {38:0.8f}, p_oneMin_avgPrice = {39:0.8f}, p_oneMin_highPrice = {40:0.8f}, p_oneMin_lowPrice = {41:0.8f}, p_oneMin_startPrice = {42:0.8f}, p_oneMin_endPrice = {43:0.8f}, p_oneMin_changeReal = {44:0.8f}, p_oneMin_changePercent = {45:0.8f}, p_oneMin_travelReal = {46:0.8f}, p_oneMin_travelPercent = {47:0.8f}, p_oneMin_volumePrMin = {48:0.8f}, p_oneMin_tradesPrMin = {49:0.8f}, p_oneMin_buysPrMin = {50:0.8f}, p_oneMin_sellsPrMin = {51:0.8f}, p_threeMin_avgPrice = {52:0.8f}, p_threeMin_highPrice = {53:0.8f}, p_threeMin_lowPrice = {54:0.8f}, p_threeMin_startPrice = {55:0.8f}, p_threeMin_endPrice = {56:0.8f}, p_threeMin_changeReal = {57:0.8f}, p_threeMin_changePercent = {58:0.8f}, p_threeMin_travelReal = {59:0.8f}, p_threeMin_travelPercent = {60:0.8f}, p_threeMin_volumePrMin = {61:0.8f}, p_threeMin_tradesPrMin = {62:0.8f}, p_threeMin_buysPrMin = {63:0.8f}, p_threeMin_sellsPrMin = {64:0.8f}, p_fiveMin_avgPrice = {65:0.8f}, p_fiveMin_highPrice = {66:0.8f}, p_fiveMin_lowPrice = {67:0.8f}, p_fiveMin_startPrice = {68:0.8f}, p_fiveMin_endPrice = {69:0.8f}, p_fiveMin_changeReal = {70:0.8f}, p_fiveMin_changePercent = {71:0.8f}, p_fiveMin_travelReal = {72:0.8f}, p_fiveMin_travelPercent = {73:0.8f}, p_fiveMin_volumePrMin = {74:0.8f}, p_fiveMin_tradesPrMin = {75:0.8f}, p_fiveMin_buysPrMin = {76:0.8f}, p_fiveMin_sellsPrMin = {77:0.8f}, p_tenMin_avgPrice = {78:0.8f}, p_tenMin_highPrice = {79:0.8f}, p_tenMin_lowPrice = {80:0.8f}, p_tenMin_startPrice = {81:0.8f}, p_tenMin_endPrice = {82:0.8f}, p_tenMin_changeReal = {83:0.8f}, p_tenMin_changePercent = {84:0.8f}, p_tenMin_travelReal = {85:0.8f}, p_tenMin_travelPercent = {86:0.8f}, p_tenMin_volumePrMin = {87:0.8f}, p_tenMin_tradesPrMin = {88:0.8f}, p_tenMin_buysPrMin = {89:0.8f}, p_tenMin_sellsPrMin = {90:0.8f}, p_fifteenMin_avgPrice = {91:0.8f}, p_fifteenMin_highPrice = {92:0.8f}, p_fifteenMin_lowPrice = {93:0.8f}, p_fifteenMin_startPrice = {94:0.8f}, p_fifteenMin_endPrice = {95:0.8f}, p_fifteenMin_changeReal = {96:0.8f}, p_fifteenMin_changePercent = {97:0.8f}, p_fifteenMin_travelReal = {98:0.8f}, p_fifteenMin_travelPercent = {99:0.8f}, p_fifteenMin_volumePrMin = {100:0.8f}, p_fifteenMin_tradesPrMin = {101:0.8f}, p_fifteenMin_buysPrMin = {102:0.8f}, p_fifteenMin_sellsPrMin = {103:0.8f}, p_thirtyMin_avgPrice = {104:0.8f}, p_thirtyMin_highPrice = {105:0.8f}, p_thirtyMin_lowPrice = {106:0.8f}, p_thirtyMin_startPrice = {107:0.8f}, p_thirtyMin_endPrice = {108:0.8f}, p_thirtyMin_changeReal = {109:0.8f}, p_thirtyMin_changePercent = {110:0.8f}, p_thirtyMin_travelReal = {111:0.8f}, p_thirtyMin_travelPercent = {112:0.8f}, p_thirtyMin_volumePrMin = {113:0.8f}, p_thirtyMin_tradesPrMin = {114:0.8f}, p_thirtyMin_buysPrMin = {115:0.8f}, p_thirtyMin_sellsPrMin = {116:0.8f}, p_sixtyMin_avgPrice = {117:0.8f}, p_sixtyMin_highPrice = {118:0.8f}, p_sixtyMin_lowPrice = {119:0.8f}, p_sixtyMin_startPrice = {120:0.8f}, p_sixtyMin_endPrice = {121:0.8f}, p_sixtyMin_changeReal = {122:0.8f}, p_sixtyMin_changePercent = {123:0.8f}, p_sixtyMin_travelReal = {124:0.8f}, p_sixtyMin_travelPercent = {125:0.8f}, p_sixtyMin_volumePrMin = {126:0.8f}, p_sixtyMin_tradesPrMin = {127:0.8f}, p_sixtyMin_buysPrMin = {128:0.8f}, p_sixtyMin_sellsPrMin = {129:0.8f}, p_oneTwentyMin_avgPrice = {130:0.8f}, p_oneTwentyMin_highPrice = {131:0.8f}, p_oneTwentyMin_lowPrice = {132:0.8f}, p_oneTwentyMin_startPrice = {133:0.8f}, p_oneTwentyMin_endPrice = {134:0.8f}, p_oneTwentyMin_changeReal = {135:0.8f}, p_oneTwentyMin_changePercent = {136:0.8f}, p_oneTwentyMin_travelReal = {137:0.8f}, p_oneTwentyMin_travelPercent = {138:0.8f}, p_oneTwentyMin_volumePrMin = {139:0.8f}, p_oneTwentyMin_tradesPrMin = {140:0.8f}, p_oneTwentyMin_buysPrMin = {141:0.8f}, p_oneTwentyMin_sellsPrMin = {142:0.8f} WHERE id = {143:d}'.format(data[0]['avgPrice'],data[0]['highPrice'],data[0]['lowPrice'],data[0]['startPrice'],data[0]['endPrice'],data[0]['changeReal'],data[0]['changePercent'],data[0]['travelReal'],data[0]['travelPercent'],data[0]['volumePrMinute'],data[0]['tradesPrMinute'],data[0]['buysPrMinute'],data[0]['sellsPrMinute'],data[1]['avgPrice'],data[1]['highPrice'],data[1]['lowPrice'],data[1]['startPrice'],data[1]['endPrice'],data[1]['changeReal'],data[1]['changePercent'],data[1]['travelReal'],data[1]['travelPercent'],data[1]['volumePrMinute'],data[1]['tradesPrMinute'],data[1]['buysPrMinute'],data[1]['sellsPrMinute'],data[2]['avgPrice'],data[2]['highPrice'],data[2]['lowPrice'],data[2]['startPrice'],data[2]['endPrice'],data[2]['changeReal'],data[2]['changePercent'],data[2]['travelReal'],data[2]['travelPercent'],data[2]['volumePrMinute'],data[2]['tradesPrMinute'],data[2]['buysPrMinute'],data[2]['sellsPrMinute'],data[3]['avgPrice'],data[3]['highPrice'],data[3]['lowPrice'],data[3]['startPrice'],data[3]['endPrice'],data[3]['changeReal'],data[3]['changePercent'],data[3]['travelReal'],data[3]['travelPercent'],data[3]['volumePrMinute'],data[3]['tradesPrMinute'],data[3]['buysPrMinute'],data[3]['sellsPrMinute'],data[4]['avgPrice'],data[4]['highPrice'],data[4]['lowPrice'],data[4]['startPrice'],data[4]['endPrice'],data[4]['changeReal'],data[4]['changePercent'],data[4]['travelReal'],data[4]['travelPercent'],data[4]['volumePrMinute'],data[4]['tradesPrMinute'],data[4]['buysPrMinute'],data[4]['sellsPrMinute'],data[5]['avgPrice'],data[5]['highPrice'],data[5]['lowPrice'],data[5]['startPrice'],data[5]['endPrice'],data[5]['changeReal'],data[5]['changePercent'],data[5]['travelReal'],data[5]['travelPercent'],data[5]['volumePrMinute'],data[5]['tradesPrMinute'],data[5]['buysPrMinute'],data[5]['sellsPrMinute'],data[6]['avgPrice'],data[6]['highPrice'],data[6]['lowPrice'],data[6]['startPrice'],data[6]['endPrice'],data[6]['changeReal'],data[6]['changePercent'],data[6]['travelReal'],data[6]['travelPercent'],data[6]['volumePrMinute'],data[6]['tradesPrMinute'],data[6]['buysPrMinute'],data[6]['sellsPrMinute'],data[7]['avgPrice'],data[7]['highPrice'],data[7]['lowPrice'],data[7]['startPrice'],data[7]['endPrice'],data[7]['changeReal'],data[7]['changePercent'],data[7]['travelReal'],data[7]['travelPercent'],data[7]['volumePrMinute'],data[7]['tradesPrMinute'],data[7]['buysPrMinute'],data[7]['sellsPrMinute'],data[8]['avgPrice'],data[8]['highPrice'],data[8]['lowPrice'],data[8]['startPrice'],data[8]['endPrice'],data[8]['changeReal'],data[8]['changePercent'],data[8]['travelReal'],data[8]['travelPercent'],data[8]['volumePrMinute'],data[8]['tradesPrMinute'],data[8]['buysPrMinute'],data[8]['sellsPrMinute'],data[9]['avgPrice'],data[9]['highPrice'],data[9]['lowPrice'],data[9]['startPrice'],data[9]['endPrice'],data[9]['changeReal'],data[9]['changePercent'],data[9]['travelReal'],data[9]['travelPercent'],data[9]['volumePrMinute'],data[9]['tradesPrMinute'],data[9]['buysPrMinute'],data[9]['sellsPrMinute'],data[10]['avgPrice'],data[10]['highPrice'],data[10]['lowPrice'],data[10]['startPrice'],data[10]['endPrice'],data[10]['changeReal'],data[10]['changePercent'],data[10]['travelReal'],data[10]['travelPercent'],data[10]['volumePrMinute'],data[10]['tradesPrMinute'],data[10]['buysPrMinute'],data[10]['sellsPrMinute'],id_col))
    conn.commit()

while True:
    start = timer()
    conn = mydb.connect()

    # Get Cursor
    cur = conn.cursor()
    cur.execute("SELECT id, date_ms FROM trades WHERE p_oneTwentyMin_changePercent IS NULL ORDER BY date_ms DESC LIMIT 100,100")
    rows = cur.fetchall()


    for row in rows:
        
        getCalcAllTrades(market, row[0], row[1])
        # getCalcTrades(market, 'fiveSeconds', row[0], row[1])
        # getCalcTrades(market, 'tenSeconds', row[0], row[1])
        # getCalcTrades(market, 'thirtySeconds', row[0], row[1])
        # getCalcTrades(market, 'oneMinute', row[0], row[1])
        # getCalcTrades(market, 'threeMinute', row[0], row[1])
        # getCalcTrades(market, 'fiveMinute', row[0], row[1])
        # getCalcTrades(market, 'tenMinute', row[0], row[1])
        # getCalcTrades(market, 'fifteenMinute', row[0], row[1])
        # getCalcTrades(market, 'thirtyMinute', row[0], row[1])
        # getCalcTrades(market, 'sixtyMinute', row[0], row[1])
        # getCalcTrades(market, 'oneTwentyMinute', row[0], row[1])

    cur.close()
    conn.close()
    end = timer()
    print(datetime.fromtimestamp(round(rows[0][1] / 1000)).astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%p"))
    print("elapsed: ",timedelta(seconds=end-start))
    # print("Cruncher Takin a break.")
    # time.sleep(1)




