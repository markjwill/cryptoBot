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
import copy

# trades[0] price
# trades[1] amount
# trades[2] type
# trades[3] date_ms

# trades must always be in Ascending order

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
    'buyVsSell' : 0, #sum of buys as +1 and sells as -1
    'buyVsSellVolume' : 0,  #sum of buys as +volume and sells as -volume
    'tradeCount' : 0 #number of trades
}

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

def calculateFeatures(df, tradePool, tradeTimeMilliSeconds):
    allFeatures= {}

    for name, periodMilliseconds in TIME_PERIODS.items():
        startTimeMilliSeconds = tradeTimeMilliSeconds - PeriodMilliSeconds
        endTimeMilliseconds = tradeTimeMilliSeconds
        periodTrades = tradePool.selectTrades(name, startTimeMilliSeconds, endTimeMilliSeconds)
        pastFeatures = calculatePastFeatures(periodTrades, PeriodMilliSeconds)
        pastFeatures = {f'past_{name}_{k}': v for k, v in pastFeatures.items()}
        allFeatures = allFeatures | pastFeatures

        startTimeMilliSeconds = tradeTimeMilliSeconds 
        endTimeMilliseconds = tradeTimeMilliSeconds + PeriodMilliSeconds
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
    features = copy.copy(PERIOD_FEATURES)
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





