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

PRICE_PERIOD_FEATURES = {
    'highPrice'        : 0.0, #high price
    'lowPrice'         : 0.0, #low price
    'startPrice'       : 0.0, #start price
    'endPrice'         : 0.0, #end price
    'changeReal'       : 0.0, #start to end change price
    'changePercent'    : 0.0, #start to end change price percent
    'travelReal'       : 0.0, #low to high change in price
    'travelPercent'    : 0.0, #low to high change price percent
    'upVsDown'         : 0, #sum of price increases as +1 and price decreases as -1
}

RICH_PERIOD_FEATURES {
    'avgByTimePrice'   : 0.0, #avg Price by time
    'avgByVolumePrice' : 0.0, #avg Price by volume
    'avgByTradesPrice' : 0.0, #avg Price by number of trades
    'volume'           : 0.0, #sum of trade amounts
    'volumePrMinute'   : 0.0, #sum of trade amounts per minute
    'sellsPrMinute'    : 0.0, #number of sells per minute
    'sells'            : 0, #number of sells
    'buysPrMinute'     : 0.0, #number of buys per minute
    'buys'             : 0, #number of buys
    'buyVsSell'        : 0, #sum of buys as +1 and sells as -1
    'buyVsSellVolume'  : 0.0, #sum of buys as +volume and sells as -volume
    'tradesPrMinute'   : 0.0, #number of trades per minute
    'tradeCount'       : 0, #number of trades
}

FEATURE_INDEXES = {
    'exchange' : { 'price' : 0, 'volume' : 1,     'type' : 2    , 'quantity' : True  },
    'coinbase' : { 'price' : 5, 'volume' : False, 'type' : False, 'quantity' : False },
    'huobi'    : { 'price' : 6, 'volume' : False, 'type' : False, 'quantity' : False },
    'binance'  : { 'price' : 7, 'volume' : False, 'type' : False, 'quantity' : False },
}

NON_PERIOD_FEATURES = {
    'secondsIntoDaySin' : 0, # seconds in day sin
    'secondsIntoDayCos' : 0, # seconds in day cos
    'dayIntoWeekSin' : 0, # day in week sin
    'dayIntoWeekCos' : 0, # day in week cos
    'dayIntoYearSin' : 0, # day in year sin
    'dayIntoYearCos' : 0, # day in year cos
    'volume' : 0, # amount traded
    'type' : 0 # 1 = buy, -1 = sell
}

COLUMNS = []
FEATURES = {}

def initFeatures():
    if COLUMNS:
        return
    if FEATURES:
        return
    for source, settings in FEATURE_INDEXES.items():
        if settings['price'] is not False:
            for feature, default in PRICE_PERIOD_FEATURES.items()
                name = f'{source}_{feature}'
                FEATURES[name] = default
                COLUMNS.append(name)
        if settings['price'] is not False
                and settings['volume'] is not False
                and settings['type'] is not False
                and settings['quantity'] is not False:
            for feature, default in RICH_PERIOD_FEATURES.items()
                name = f'{source}_{feature}'
                FEATURES[name] = default
                COLUMNS.append(name)

def calculatePeriodFeatures(timeGroup, trades, milliseconds):
    if timeGroup == 'future':
        richFeatures = {
            'endPrice' : trades[-1][0], #end price
        }
        return richFeatures

    features = copy.copy(FEATURES)

    firstTrade = trades[0]
    lastTrade = trades[-1]

    volumeAtPrice = {}
    priceSum = {}

    for name, settings in FEATURE_INDEXES.items():
        if settings['price'] is not False:
            features[f'{name}_startPrice'] = firstTrade[settings['price']]
            features[f'{name}_endPrice'] = lastTrade[settings['price']]
            if float(features[f'{name}_startPrice']) == 0.0:
                raise AssertionError(
                    f'Start price feature calculated as 0 at tradeId {firstTrade[4]}.\n'
                )
            if float(features[f'{name}_endPrice']) == 0.0:
                raise AssertionError(
                    f'End price feature calculated as 0 at tradeId {lastTrade[4]}.\n'
                )
        if settings['price'] is not False 
                and settings['volume'] is not False 
                and settings['type'] is not False 
                and settings['quantity'] is not False :
            volumeAtPrice[name] = 0.0
            priceSum[name] = 0.0

    previousTrade = firstTrade



    for trade in trades:
        for name, settings in FEATURE_INDEXES.items():
            if settings['price'] is not False:
                # do price only stuff
                if trade[FEATURE_INDEXES[name]['price']] > features[f'{name}_highPrice']:
                    features[f'{name}_highPrice'] = trade[FEATURE_INDEXES[name]['price']]

                if trade[FEATURE_INDEXES[name]['price']] < features[f'{name}_lowPrice'] 
                        or features[f'{name}_lowPrice'] == 0.0:
                    features[f'{name}_lowPrice'] = trade[FEATURE_INDEXES[name]['price']]

            if settings['price'] is not False 
                    and settings['volume'] is not False 
                    and settings['type'] is not False 
                    and settings['quantity'] is not False :
                # do rich stuff
                features[f'{name}_tradeCount'] += 1
                priceSum[name] += trade[FEATURE_INDEXES[name]['price']]
                volumeAtPrice[name] += trade[FEATURE_INDEXES[name]['volume']] * trade[FEATURE_INDEXES[name]['price']]

                features[f'{name}_volume'] += trade[FEATURE_INDEXES[name]['volume']]



        if trade[2] == 'buy':
            richFeatures['buys'] += 1
            richFeatures['buyVsSell'] += 1
            richFeatures['buyVsSellVolume'] += trade[1]

        if trade[2] == 'sell':
            richFeatures['sells'] += 1
            richFeatures['buyVsSell'] -= 1
            richFeatures['buyVsSellVolume'] -= trade[1]

        if trade[0] > previousTrade[0]:
            richFeatures['upVsDown'] += 1

        if trade[0] < previousTrade[0]:
            richFeatures['upVsDown'] -= 1

    richFeatures['avgByVolumePrice'] = volumeAtPrice / richFeatures['volume']
    richFeatures['avgByTradesPrice'] = priceSum / richFeatures['tradeCount']
    richFeatures['volumePrMinute'] = richFeatures['volume'] / ( milliseconds / 60000 )
    richFeatures['changeReal'] = richFeatures['endPrice'] - richFeatures['startPrice']
    richFeatures['changePercent'] = richFeatures['changeReal'] * 100 / richFeatures['startPrice']
    richFeatures['travelReal'] = richFeatures['highPrice'] - richFeatures['lowPrice']
    richFeatures['travelPercent'] = richFeatures['travelReal'] * 100 / richFeatures['lowPrice']
    richFeatures['tradesPrMinute'] = richFeatures['tradeCount'] / ( milliseconds / 60000 )
    richFeatures['buysPrMinute'] = richFeatures['buys'] / ( milliseconds / 60000 )
    richFeatures['sellsPrMinute'] = richFeatures['sells'] / ( milliseconds / 60000 )

    richFeatures['avgByVolumePrice'] = lastTrade[0] - richFeatures['avgByVolumePrice']
    richFeatures['avgByTradesPrice'] = lastTrade[0] - richFeatures['avgByTradesPrice']
    richFeatures['lowPrice'] = lastTrade[0] - richFeatures['lowPrice']
    richFeatures['highPrice'] = lastTrade[0] - richFeatures['highPrice']
    richFeatures['startPrice'] = lastTrade[0] - richFeatures['startPrice']
    richFeatures['endPrice'] = lastTrade[0] - richFeatures['endPrice']

    return richFeatures

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
        pastFeatures = calculateFeatureGroup(timeGroup, name, tradePool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds)
        allFeatures = allFeatures | pastFeatures

        timeGroup = 'future'
        startTimeMilliseconds = tradeTimeMilliseconds 
        endTimeMilliseconds = tradeTimeMilliseconds + periodMilliseconds
        futureFeatures = calculateFeatureGroup(timeGroup, name, tradePool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds)
        allFeatures = allFeatures | futureFeatures

    data = { pivotTradeId: list(allFeatures.values()) }
    concatDf = pd.DataFrame.from_dict(data, orient='index', columns=COLUMNS)
    df = pd.concat([df, concatDf])
    logging.debug('All feature groups calculated')
    return df

def calculateFeatureGroup(timeGroup, name, tradePool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds):
    logging.debug(f'Collecting trades and calculating Group {timeGroup}_{name}')
    periodTrades = tradePool.getTrades(f'{timeGroup}_{name}', timeGroup, pivotTradeId, startTimeMilliseconds, endTimeMilliseconds)
    periodFeatures = calculatePeriodFeatures(timeGroup, periodTrades, endTimeMilliseconds - startTimeMilliseconds)
    periodFeatures = {f'{timeGroup}_{name}_{k}': v for k, v in periodFeatures.items()}

    return periodFeatures





