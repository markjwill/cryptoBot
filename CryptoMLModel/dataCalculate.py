import pandas as pd
import copy
import logging

# trade[0] price
# trade[1] amount
# trade[2] type
# trade[3] date_ms
# trade[4] trade_id

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
    # 'fiveSeconds': 5000,
    'tenSeconds': 10000,
    'thirtySeconds': 30000,
    'oneMinute': 60000,
    'threeMinutes': 180000,
    'fiveMinutes': 300000,
    'tenMinutes': 600000,
    'fifteenMinutes': 900000,
    'thirtyMinutes': 1800000,
    'sixtyMinutes': 3600000,
    'oneTwentyMinutes': 7200000
}

MAX_PERIOD = 7200000

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
    'volumePrMillisecond' : 0.0, #sum of trade amounts per minute
    'sellsPrMillisecond' : 0.0, #number of sells per minute
    'sells' : 0, #number of sells
    'buysPrMillisecond' : 0.0, #number of buys per minute
    'buys' : 0, #number of buys
    'buyVsSell' : 0, #sum of buys as +1 and sells as -1
    'buyVsSellVolume' : 0,  #sum of buys as +volume and sells as -volume
    'tradesPrMillisecond' : 0, #number of trades per minute
    'tradeCount' : 0 #number of trades
}

columns = []

def calculateFeatures(timeGroup, trades, milliseconds):
    if timeGroup == 'future':
        features = {
            'endPrice' : trades[-1][0], #end price
        }
        return features

    features = copy.copy(PERIOD_FEATURES)
    features['startPrice'] = trades[0][0]
    features['endPrice'] = trades[-1][0]
    volumeAtPrice = 0

    for trade in trades:
        features['tradeCount'] += 1
        volumeAtPrice += trade[1] * trade[0]

        if trade[0] > features['highPrice']:
            features['highPrice'] = trade[0]

        if trade[0] < features['lowPrice'] or features['lowPrice'] == 0.0:
            features['lowPrice'] = trade[0]

        features['volume'] += trade[1]

        if trade[2] == 'buy':
            features['buys'] += 1
            features['buyVsSell'] += 1
            features['buyVsSellVolume'] += trade[1]

        if trade[2] == 'sell':
            features['sells'] += 1
            features['buyVsSell'] -= 1
            features['buyVsSellVolume'] -= trade[1]

    features['avgPrice'] = volumeAtPrice / features['volume']
    features['volumePrMillisecond'] = features['volume'] / milliseconds
    features['changeReal'] = features['endPrice'] - features['startPrice']
    features['changePercent'] = features['changeReal'] * 100 / features['startPrice']
    features['travelReal'] = features['highPrice'] - features['lowPrice']
    features['travelPercent'] = features['travelReal'] * 100 / features['lowPrice']
    features['tradesPrMillisecond'] = features['tradeCount'] / milliseconds
    features['buysPrMillisecond'] = features['buys'] / milliseconds
    features['sellsPrMillisecond'] = features['sells'] / milliseconds

    if float(features['startPrice']) == 0.0:
        raise AssertionError(
            f'Start price feature calculated as {features[featureName]}.\n'
            'Ensure every defined PERIOD_FEATURES has a calculation defined in calculateFeatures'
        )

    if float(features['endPrice']) == 0.0:
        raise AssertionError(
            f'End price feature calculated as {features[featureName]}.\n'
            'Ensure every defined PERIOD_FEATURES has a calculation defined in calculateFeatures'
        )

    return features

def setupDataFrame():
    df = pd.DataFrame()
    for periodName in TIME_PERIODS:
        for featureName in PERIOD_FEATURES:
            columns.append(f'past_{periodName}_{featureName}')
        columns.append(f'future_{periodName}_endPrice')
    logging.debug(f"Dataframe colums set as: {*columns,}")
    return df

def calculateAllFeatureGroups(df, tradePool, pivotTrade):
    tradeTimeMilliseconds = pivotTrade[3]
    pivotTradeId = pivotTrade[4]
    allFeatures = {}
    for name, periodMilliseconds in TIME_PERIODS.items():
        timeGroup = 'past'
        startTimeMilliseconds = tradeTimeMilliseconds - periodMilliseconds
        endTimeMilliseconds = tradeTimeMilliseconds
        pastFeatures = calculateFeatureGroup(timeGroup, name, tradePool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds)
        allFeatures = allFeatures | pastFeatures

        timeGroup = 'future'
        startTimeMilliseconds = tradeTimeMilliseconds 
        endTimeMilliseconds = tradeTimeMilliseconds + periodMilliseconds
        futureFeatures = calculateFeatureGroup(timeGroup, name, tradePool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds)
        allFeatures = allFeatures | futureFeatures

    data = { pivotTradeId: list(allFeatures.values()) }
    concatDf = pd.DataFrame.from_dict(data, orient='index', columns=columns)
    df = pd.concat([df, concatDf])
    logging.info('All feature groups calculated')
    return df

def calculateFeatureGroup(timeGroup, name, tradePool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds):
    logging.info(f'Collecting trades and calculating Group {timeGroup}_{name}')
    periodTrades = tradePool.getTrades(f'{timeGroup}_{name}', timeGroup, pivotTradeId, startTimeMilliseconds, endTimeMilliseconds)
    periodFeatures = calculateFeatures(timeGroup, periodTrades, endTimeMilliseconds - startTimeMilliseconds)
    periodFeatures = {f'{timeGroup}_{name}_{k}': v for k, v in periodFeatures.items()}

    return periodFeatures





