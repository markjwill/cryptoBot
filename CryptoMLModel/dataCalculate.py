import pandas as pd
import copy
import logging
import datetime
import numpy as np

# trade[0] price
# trade[1] amount
# trade[2] type
# trade[3] date_ms
# trade[4] trade_id
# trades[5] coinbasePrice
# trades[6] huobiPrice
# trades[7] binancePrice

TIME_PERIODS = {
    'tenSeconds'       : 10000,
    'thirtySeconds'    : 30000,
    'ninetySeconds'    : 90000,
    'fiveMinutes'      : 300000,
    'fifteenMinutes'   : 900000,
    'fortyFiveMinutes' : 2700000,
    'twoHours'         : 7200000
}

MAX_PERIOD = 7200000

PRICE_PERIOD_FEATURES = {
    'avgByTradesPrice' : 0.0, #avg Price by number of trades
    'highPrice'        : 0.0, #high price
    'lowPrice'         : 0.0, #low price
    'startPrice'       : 0.0, #start price
    'changeReal'       : 0.0, #start to end change price
    'changePercent'    : 0.0, #start to end change price percent
    'travelReal'       : 0.0, #low to high change in price
    'travelPercent'    : 0.0, #low to high change price percent
    'upVsDown'         : 0,   #sum of price increases as +1 and price decreases as -1
}

RICH_PERIOD_FEATURES = {
    'avgByVolumePrice' : 0.0, #avg Price by volume
    'volume'           : 0.0, #sum of trade amounts
    'volumePrMinute'   : 0.0, #sum of trade amounts per minute
    'sellsPrMinute'    : 0.0, #number of sells per minute
    'sells'            : 0,   #number of sells
    'buysPrMinute'     : 0.0, #number of buys per minute
    'buys'             : 0,   #number of buys
    'buyVsSell'        : 0,   #sum of buys as +1 and sells as -1
    'buyVsSellVolume'  : 0.0, #sum of buys as +volume and sells as -volume
    'tradesPrMinute'   : 0.0, #number of trades per minute
}

SINGLE_PERIOD_FEATURES = {
    'tradeCount' : 0, #number of trades
}

FEATURE_INDEXES = {
    'exchange' : { 'price' : 0, 'volume' : 1,     'type' : 2,     'quantity' : True  },
    'coinbase' : { 'price' : 5, 'volume' : False, 'type' : False, 'quantity' : False },
    'huobi'    : { 'price' : 6, 'volume' : False, 'type' : False, 'quantity' : False },
    'binance'  : { 'price' : 7, 'volume' : False, 'type' : False, 'quantity' : False },
}

NON_PERIOD_FEATURES = {
    'secondsIntoDaySin' : 0, # seconds in day sin
    'secondsIntoDayCos' : 0, # seconds in day cos
    'dayIntoWeekSin'    : 0, # day in week sin
    'dayIntoWeekCos'    : 0, # day in week cos
    'dayIntoYearSin'    : 0, # day in year sin
    'dayIntoYearCos'    : 0, # day in year cos
    'volume'            : 0, # amount traded
    'type'              : 0 # 1 = buy, -1 = sell
}

COLUMNS = []
FEATURES = {}

def initFeatures():
    if COLUMNS:
        return
    if FEATURES:
        return

    for featureName, default in SINGLE_PERIOD_FEATURES.items():
        FEATURES[featureName] = default

    for sourceName, index in FEATURE_INDEXES.items():
        if index['price'] is not False:
            for feature, default in PRICE_PERIOD_FEATURES.items():
                FEATURES[f'{sourceName}_{feature}'] = default
        if    index['price'] is not False and \
             index['volume'] is not False and \
               index['type'] is not False and \
           index['quantity'] is not False:
            for feature, default in RICH_PERIOD_FEATURES.items():
                FEATURES[f'{sourceName}_{feature}'] = default

    for featureName, default in NON_PERIOD_FEATURES.items():
        COLUMNS.append(featureName)

    for timeName in TIME_PERIODS:
        for featureName in FEATURES:
            COLUMNS.append(f'{timeName}_{featureName}')
        COLUMNS.append(f'{timeName}_futurePrice')

    logging.debug('COLUMNS:')
    logging.debug(COLUMNS)
    logging.debug('FEATURES:')
    logging.debug(FEATURES)

def addFeatureName(name, default):
    FEATURES[name] = default
    COLUMNS.append(name)

def calculateFuturePeriodFeatures(trades, pivotPrice):
    if not trades:
        raise AssertionError(
            f'Empty trade list sent to feature calculation {firstTrade[4]}.\n'
        )

    features = {
        'futurePrice' : pivotPrice - trades[-1][0], #end price
    }
    return features

def calculatePastPeriodFeatures(trades, milliseconds):
    if not trades:
        raise AssertionError(
            f'Empty trade list sent to feature calculation {firstTrade[4]}.\n'
        )

    features = copy.copy(FEATURES)

    firstTrade = trades[0]
    lastTrade = trades[-1]
    tradeCount = len(trades)

    volumeAtPrice = {}
    priceSum = {}
    endPrice = {}

    features['tradeCount'] = len(trades)

    for sourceName, index in FEATURE_INDEXES.items():
        logging.debug(index)
        if index['price'] is not False:
            features[f'{sourceName}_startPrice'] = firstTrade[index['price']]
            endPrice[sourceName] = lastTrade[index['price']]
            priceSum[sourceName] = 0.0
            if float(features[f'{sourceName}_startPrice']) == 0.0:
                raise AssertionError(
                    f'Start price feature calculated as 0 at tradeId {firstTrade[4]}.\n'
                )
            if float(endPrice[sourceName]) == 0.0:
                raise AssertionError(
                    f'End price feature calculated as 0 at tradeId {lastTrade[4]}.\n'
                )
        if    index['price'] is not False and \
             index['volume'] is not False and \
               index['type'] is not False and \
           index['quantity'] is not False:
            volumeAtPrice[sourceName] = 0.0

    previousTrade = firstTrade

    for trade in trades:
        for sourceName, index in FEATURE_INDEXES.items():
            if index['price'] is not False:
                if trade[index['price']] > features[f'{sourceName}_highPrice']:
                    features[f'{sourceName}_highPrice'] = trade[index['price']]

                if trade[index['price']] < features[f'{sourceName}_lowPrice'] \
                        or features[f'{sourceName}_lowPrice'] == 0.0:
                    features[f'{sourceName}_lowPrice'] = trade[index['price']]

                if trade[index['price']] > previousTrade[index['price']]:
                    features[f'{sourceName}_upVsDown'] += 1

                if trade[index['price']] < previousTrade[index['price']]:
                    features[f'{sourceName}_upVsDown'] -= 1

                priceSum[sourceName] += trade[index['price']]

            if    index['price'] is not False and \
                 index['volume'] is not False and \
                   index['type'] is not False and \
               index['quantity'] is not False:
                volumeAtPrice[sourceName] += trade[index['volume']] * trade[index['price']]

                features[f'{sourceName}_volume'] += trade[index['volume']]

                if trade[index['type']] == 'buy':
                    features[f'{sourceName}_buys'] += 1
                    features[f'{sourceName}_buyVsSell'] += 1
                    features[f'{sourceName}_buyVsSellVolume'] += trade[index['volume']]

                if trade[index['type']] == 'sell':
                    features[f'{sourceName}_sells'] += 1
                    features[f'{sourceName}_buyVsSell'] -= 1
                    features[f'{sourceName}_buyVsSellVolume'] -= trade[index['volume']]

        previousTrade = trade

    for sourceName, index in FEATURE_INDEXES.items():
        pivotPrice = endPrice[sourceName]
        if index['price'] is not False:
            features[f'{sourceName}_changeReal'] = pivotPrice - features[f'{sourceName}_startPrice']
            features[f'{sourceName}_changePercent'] = features[f'{sourceName}_changeReal'] * 100 / features[f'{sourceName}_startPrice']
            features[f'{sourceName}_travelReal'] = features[f'{sourceName}_highPrice'] - features[f'{sourceName}_lowPrice']
            features[f'{sourceName}_travelPercent'] = features[f'{sourceName}_travelReal'] * 100 / features[f'{sourceName}_lowPrice']

            features[f'{sourceName}_avgByTradesPrice'] = pivotPrice - ( priceSum[sourceName] / tradeCount )
            features[f'{sourceName}_lowPrice'] = pivotPrice - features[f'{sourceName}_lowPrice']
            features[f'{sourceName}_highPrice'] = pivotPrice - features[f'{sourceName}_highPrice']
            features[f'{sourceName}_startPrice'] = pivotPrice - features[f'{sourceName}_startPrice']

        if    index['price'] is not False and \
             index['volume'] is not False and \
               index['type'] is not False and \
           index['quantity'] is not False:
            features[f'{sourceName}_volumePrMinute'] = features[f'{sourceName}_volume'] / ( milliseconds / 60000 )
            features[f'{sourceName}_tradesPrMinute'] = features['tradeCount'] / ( milliseconds / 60000 )
            features[f'{sourceName}_buysPrMinute'] = features[f'{sourceName}_buys'] / ( milliseconds / 60000 )
            features[f'{sourceName}_sellsPrMinute'] = features[f'{sourceName}_sells'] / ( milliseconds / 60000 )
            features[f'{sourceName}_avgByVolumePrice'] = pivotPrice - ( volumeAtPrice[sourceName] / features[f'{sourceName}_volume'] )

    return features, endPrice['exchange']

def calculateNonPeriodFeatures(trade):
    features = copy.copy(NON_PERIOD_FEATURES)
    dt = datetime.datetime.fromtimestamp(trade[3]/1000)
    t = dt.time()
    secondsIntoDay = (t.hour * 60 + t.minute) * 60 + t.second
    dayIntoYear = dt.timetuple().tm_yday
    features['secondsIntoDaySin'] = np.sin(secondsIntoDay * (2 * np.pi / 86400)) # seconds_in_day_sin
    features['secondsIntoDayCos'] = np.cos(secondsIntoDay * (2 * np.pi / 86400)) # seconds_in_day_cos
    features['dayIntoWeekSin'] = np.sin(dt.weekday() * (2 * np.pi / 7)) # day_in_week_sin
    features['dayIntoWeekCos'] = np.cos(dt.weekday() * (2 * np.pi / 7)) # day_in_week_cos
    features['dayIntoYearSin'] = np.sin(dayIntoYear * (2 * np.pi / 365)) # day_in_year_sin
    features['dayIntoYearCos'] = np.cos(dayIntoYear * (2 * np.pi / 365)) # day_in_year_cos
    features['volume'] = trade[1] # amount_traded
    features['type'] = 1 if trade[4] == 'buy' else -1 # type

    return features

def setupDataFrame():
    df = pd.DataFrame()
    initFeatures()
    logging.debug(f"Dataframe colums set as: {*COLUMNS,}")
    return df

def calculateAllFeatureGroups(df, tradePool, pivotTrade):
    tradeTimeMilliseconds = pivotTrade[3]
    pivotTradeId = pivotTrade[4]
    allFeatures = {}

    allFeatures = allFeatures | calculateNonPeriodFeatures(pivotTrade)

    for name, periodMilliseconds in TIME_PERIODS.items():
        timeGroup = 'past'
        startTimeMilliseconds = tradeTimeMilliseconds - periodMilliseconds
        endTimeMilliseconds = tradeTimeMilliseconds
        pastFeatures, pivotPrice = calculatePastFeatureGroup(name, tradePool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds)
        allFeatures = allFeatures | pastFeatures

        timeGroup = 'future'
        startTimeMilliseconds = tradeTimeMilliseconds 
        endTimeMilliseconds = tradeTimeMilliseconds + periodMilliseconds
        futureFeatures = calculateFutureFeatureGroup(name, tradePool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds, pivotPrice)
        allFeatures = allFeatures | futureFeatures

    data = { pivotTradeId: list(allFeatures.values()) }
    concatDf = pd.DataFrame.from_dict(data, orient='index', columns=COLUMNS)
    df = pd.concat([df, concatDf])
    logging.debug('All feature groups calculated')
    return df

def calculatePastFeatureGroup(name, tradePool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds):
    logging.debug(f'Collecting trades and calculating Group past_{name}')
    periodTrades = tradePool.getTrades(f'past_{name}', 'past', pivotTradeId, startTimeMilliseconds, endTimeMilliseconds)
    pastFeatures, pivotPrice = calculatePastPeriodFeatures(periodTrades, endTimeMilliseconds - startTimeMilliseconds)
    pastFeatures = {f'{name}_{k}': v for k, v in pastFeatures.items()}

    return pastFeatures, pivotPrice

def calculateFutureFeatureGroup(name, tradePool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds, pivotPrice):
    logging.debug(f'Collecting trades and calculating Group future_{name}')
    periodTrades = tradePool.getTrades(f'future_{name}', 'future', pivotTradeId, startTimeMilliseconds, endTimeMilliseconds)
    futureFeature = calculateFuturePeriodFeatures(periodTrades, pivotPrice)
    futureFeature = {f'{name}_{k}': v for k, v in futureFeature.items()}

    return futureFeature





