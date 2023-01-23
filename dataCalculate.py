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

# update all trade[n] syntax to use named tradePool functions instead

def calculateFuturePeriodFeatures(trades, pivotPrice):
    if not trades:
        raise AssertionError(
            f'Empty trade list sent to future feature calculation.\n'
        )

    features = {
        'futurePrice' : trades[-1][0] - pivotPrice #end price
    }
    return features

def calculatePastPeriodFeatures(trades, milliseconds, features):
    if not trades:
        raise AssertionError(
            f'Empty trade list sent to past feature calculation.\n'
        )

    features = copy.copy(features.PERIOD_FEATURES)

    firstTrade = trades[0]
    lastTrade = trades[-1]
    tradeCount = len(trades)

    volumeAtPrice = {}
    priceSum = {}
    endPrice = {}

    features['tradeCount'] = len(trades)

    for sourceName, index in features.FEATURE_INDEXES.items():
        logging.debug(index)
        if index['price'] is not False:
            features[f'{sourceName}_startPrice'] = firstTrade[index['price']]
            endPrice[sourceName] = lastTrade[index['price']]
            priceSum[sourceName] = 0.0
            if float(features[f'{sourceName}_startPrice']) == 0.0:
                raise AssertionError(
                    f'Start price feature calculated as 0 at tradeId {firstTrade[4]}.'
                )
            if float(endPrice[sourceName]) == 0.0:
                raise AssertionError(
                    f'End price feature calculated as 0 at tradeId {lastTrade[4]}.'
                )
        if    index['price'] is not False and \
             index['volume'] is not False and \
               index['type'] is not False and \
           index['quantity'] is not False:
            volumeAtPrice[sourceName] = 0.0

    previousTrade = firstTrade

    for trade in trades:
        for sourceName, index in features.FEATURE_INDEXES.items():
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

    for sourceName, index in features.FEATURE_INDEXES.items():
        pivotPrice = endPrice[sourceName]
        if index['price'] is not False:
            features[f'{sourceName}_changeReal'] = features[f'{sourceName}_startPrice'] - pivotPrice
            features[f'{sourceName}_changePercent'] = features[f'{sourceName}_changeReal'] * 100 / features[f'{sourceName}_startPrice']
            features[f'{sourceName}_travelReal'] = features[f'{sourceName}_highPrice'] - features[f'{sourceName}_lowPrice']
            features[f'{sourceName}_travelPercent'] = features[f'{sourceName}_travelReal'] * 100 / features[f'{sourceName}_lowPrice']

            features[f'{sourceName}_avgByTradesPrice'] = ( priceSum[sourceName] / tradeCount ) - pivotPrice
            features[f'{sourceName}_lowPrice'] = features[f'{sourceName}_lowPrice'] - pivotPrice
            features[f'{sourceName}_highPrice'] = features[f'{sourceName}_highPrice'] - pivotPrice
            features[f'{sourceName}_startPrice'] = features[f'{sourceName}_startPrice'] - pivotPrice

        if    index['price'] is not False and \
             index['volume'] is not False and \
               index['type'] is not False and \
           index['quantity'] is not False:
            features[f'{sourceName}_volumePrMinute'] = features[f'{sourceName}_volume'] / ( milliseconds / 60000 )
            features[f'{sourceName}_tradesPrMinute'] = features['tradeCount'] / ( milliseconds / 60000 )
            features[f'{sourceName}_buysPrMinute'] = features[f'{sourceName}_buys'] / ( milliseconds / 60000 )
            features[f'{sourceName}_sellsPrMinute'] = features[f'{sourceName}_sells'] / ( milliseconds / 60000 )
            features[f'{sourceName}_avgByVolumePrice'] = ( volumeAtPrice[sourceName] / features[f'{sourceName}_volume'] ) - pivotPrice

    return features, endPrice['exchange']

def calculateNonPeriodFeatures(trade, features):
    features = copy.copy(features.NON_PERIOD_FEATURES)
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

def calculateAllFeatureGroups(df, tradePool, features, pivotTrade):
    tradeTimeMilliseconds = pivotTrade[3]
    pivotTradeId = pivotTrade[4]
    allFeatures = {}

    allFeatures = allFeatures | calculateNonPeriodFeatures(pivotTrade, features)

    for timeName, periodMilliseconds in features.TIME_PERIODS.items():
        timeGroup = 'past'
        startTimeMilliseconds = tradeTimeMilliseconds - periodMilliseconds
        endTimeMilliseconds = tradeTimeMilliseconds
        pastFeatures, pivotPrice = calculatePastFeatureGroup(
                timeName, 
                tradePool, 
                startTimeMilliseconds, 
                pivotTradeId, 
                endTimeMilliseconds, 
                features )
        allFeatures = allFeatures | pastFeatures

        timeGroup = 'future'
        startTimeMilliseconds = tradeTimeMilliseconds 
        endTimeMilliseconds = tradeTimeMilliseconds + periodMilliseconds
        futureFeatures = calculateFutureFeatureGroup(timeName, tradePool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds, pivotPrice)
        allFeatures = allFeatures | futureFeatures

    # move dataFrame concat to a seperate function
    data = { pivotTradeId: list(allFeatures.values()) }
    concatDf = pd.DataFrame.from_dict(data, orient='index', columns=COLUMNS)
    df = pd.concat([df, concatDf])
    logging.debug('All feature groups calculated')
    return df

def calculatePastFeatureGroup(name, tradePool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds, features):
    logging.debug(f'Collecting trades and calculating Group past_{name}')
    periodTrades = tradePool.getTrades(f'past_{name}', 'past', pivotTradeId, startTimeMilliseconds, endTimeMilliseconds)
    pastFeatures, pivotPrice = calculatePastPeriodFeatures(periodTrades, endTimeMilliseconds - startTimeMilliseconds, features)
    pastFeatures = {f'{name}_{k}': v for k, v in pastFeatures.items()}

    return pastFeatures, pivotPrice

def calculateFutureFeatureGroup(name, tradePool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds, pivotPrice):
    logging.debug(f'Collecting trades and calculating Group future_{name}')
    periodTrades = tradePool.getTrades(f'future_{name}', 'future', pivotTradeId, startTimeMilliseconds, endTimeMilliseconds)
    futureFeature = calculateFuturePeriodFeatures(periodTrades, pivotPrice)
    futureFeature = {f'{name}_{k}': v for k, v in futureFeature.items()}

    return futureFeature





