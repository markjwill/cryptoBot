import copy
import logging
import datetime
import numpy as np
import pandas as pd
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

    calculatedFeatures = {
        'futurePrice' : trades[-1][0] - pivotPrice #end price
    }
    return calculatedFeatures

def calculatePastPeriodFeatures(trades, milliseconds, features):
    if not trades:
        raise AssertionError(
            f'Empty trade list sent to past feature calculation.\n'
        )

    calculatedFeatures = copy.copy(features.PERIOD_FEATURES)

    tradeArray = np.array([trades])

    firstTrade = trades[0]
    lastTrade = trades[-1]
    tradeCount = len(trades)

    volumeAtPrice = {}
    priceSum = {}
    endPrice = {}
    difference = {}
    positiveChange = {}
    negativeChange = {}

    calculatedFeatures['tradeCount'] = len(trades)

    for sourceName, index in features.FEATURE_INDEXES.items():
        logging.debug(index)
        if index['price'] is not False:
            calculatedFeatures[f'{sourceName}_startPrice'] = firstTrade[index['price']]
            endPrice[sourceName] = lastTrade[index['price']]
            # priceSum[sourceName] = 0.0
            priceSum[sourceName] = tradeArray[:, index['price']].sum()
            calculatedFeatures[f'{sourceName}_highPrice'] = np.amax(tradeArray[:, index['price']], axis=0)
            calculatedFeatures[f'{sourceName}_lowPrice'] = np.amin(tradeArray[:, index['price']], axis=0)
            difference[sourceName] = np.diff(tradeArray[:, index['price']], axis=0)
            positiveChange[sourceName] = np.sum(np.array(difference[sourceName]) >= 0, axis=0)
            negativeChange[sourceName] = positiveChange[sourceName] - tradeCount
            calculatedFeatures[f'{sourceName}_upVsDown'] = positiveChange[sourceName] + negativeChange[sourceName]
            if float(calculatedFeatures[f'{sourceName}_startPrice']) == 0.0:
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
            calculatedFeatures[f'{sourceName}_volume'] = tradeArray[:, index['volume']].sum()
            volumeAtPrice[sourceName] = np.sum(np.multiply(tradeArray[:, index['volume']], tradeArray[:, index['price']]))
            calculatedFeatures[f'{sourceName}_buys'] = np.sum(tradeArray[:,index['type']] == 'buy', axis=0)
            calculatedFeatures[f'{sourceName}_sells'] = abs(calculatedFeatures[f'{sourceName}_buys'] - tradeCount)
            calculatedFeatures[f'{sourceName}_buyVsSell'] = calculatedFeatures[f'{sourceName}_buys'] - calculatedFeatures[f'{sourceName}_sells']
    previousTrade = firstTrade

    for trade in trades:
        for sourceName, index in features.FEATURE_INDEXES.items():
            if index['price'] is not False:
                # if trade[index['price']] > calculatedFeatures[f'{sourceName}_highPrice']:
                #     calculatedFeatures[f'{sourceName}_highPrice'] = trade[index['price']]

                # if trade[index['price']] < calculatedFeatures[f'{sourceName}_lowPrice'] \
                #         or calculatedFeatures[f'{sourceName}_lowPrice'] == 0.0:
                #     calculatedFeatures[f'{sourceName}_lowPrice'] = trade[index['price']]

                # if trade[index['price']] > previousTrade[index['price']]:
                #     calculatedFeatures[f'{sourceName}_upVsDown'] += 1

                # if trade[index['price']] < previousTrade[index['price']]:
                #     calculatedFeatures[f'{sourceName}_upVsDown'] -= 1

                # priceSum[sourceName] += trade[index['price']]

            if    index['price'] is not False and \
                 index['volume'] is not False and \
                   index['type'] is not False and \
               index['quantity'] is not False:
                # volumeAtPrice[sourceName] += trade[index['volume']] * trade[index['price']]

                # calculatedFeatures[f'{sourceName}_volume'] += trade[index['volume']]

                if trade[index['type']] == 'buy':
                    # calculatedFeatures[f'{sourceName}_buys'] += 1
                    # calculatedFeatures[f'{sourceName}_buyVsSell'] += 1
                    calculatedFeatures[f'{sourceName}_buyVsSellVolume'] += trade[index['volume']]

                if trade[index['type']] == 'sell':
                    # calculatedFeatures[f'{sourceName}_sells'] += 1
                    # calculatedFeatures[f'{sourceName}_buyVsSell'] -= 1
                    calculatedFeatures[f'{sourceName}_buyVsSellVolume'] -= trade[index['volume']]

        previousTrade = trade

    for sourceName, index in features.FEATURE_INDEXES.items():
        pivotPrice = endPrice[sourceName]
        if index['price'] is not False:
            calculatedFeatures[f'{sourceName}_changeReal'] = calculatedFeatures[f'{sourceName}_startPrice'] - pivotPrice
            calculatedFeatures[f'{sourceName}_changePercent'] = calculatedFeatures[f'{sourceName}_changeReal'] * 100 / calculatedFeatures[f'{sourceName}_startPrice']
            calculatedFeatures[f'{sourceName}_travelReal'] = calculatedFeatures[f'{sourceName}_highPrice'] - calculatedFeatures[f'{sourceName}_lowPrice']
            calculatedFeatures[f'{sourceName}_travelPercent'] = calculatedFeatures[f'{sourceName}_travelReal'] * 100 / calculatedFeatures[f'{sourceName}_lowPrice']

            calculatedFeatures[f'{sourceName}_avgByTradesPrice'] = ( priceSum[sourceName] / tradeCount ) - pivotPrice
            calculatedFeatures[f'{sourceName}_lowPrice'] = calculatedFeatures[f'{sourceName}_lowPrice'] - pivotPrice
            calculatedFeatures[f'{sourceName}_highPrice'] = calculatedFeatures[f'{sourceName}_highPrice'] - pivotPrice
            calculatedFeatures[f'{sourceName}_startPrice'] = calculatedFeatures[f'{sourceName}_startPrice'] - pivotPrice

        if    index['price'] is not False and \
             index['volume'] is not False and \
               index['type'] is not False and \
           index['quantity'] is not False:
            calculatedFeatures[f'{sourceName}_volumePrMinute'] = calculatedFeatures[f'{sourceName}_volume'] / ( milliseconds / 60000 )
            calculatedFeatures[f'{sourceName}_tradesPrMinute'] = calculatedFeatures['tradeCount'] / ( milliseconds / 60000 )
            calculatedFeatures[f'{sourceName}_buysPrMinute'] = calculatedFeatures[f'{sourceName}_buys'] / ( milliseconds / 60000 )
            calculatedFeatures[f'{sourceName}_sellsPrMinute'] = calculatedFeatures[f'{sourceName}_sells'] / ( milliseconds / 60000 )
            calculatedFeatures[f'{sourceName}_avgByVolumePrice'] = ( volumeAtPrice[sourceName] / calculatedFeatures[f'{sourceName}_volume'] ) - pivotPrice

    return calculatedFeatures, endPrice['exchange']

def calculateNonPeriodFeatures(trade, features):
    calculatedFeatures = copy.copy(features.NON_PERIOD_FEATURES)
    dt = datetime.datetime.fromtimestamp(trade[3]/1000)
    t = dt.time()
    secondsIntoDay = (t.hour * 60 + t.minute) * 60 + t.second
    dayIntoYear = dt.timetuple().tm_yday
    calculatedFeatures['secondsIntoDaySin'] = np.sin(secondsIntoDay * (2 * np.pi / 86400)) # seconds_in_day_sin
    calculatedFeatures['secondsIntoDayCos'] = np.cos(secondsIntoDay * (2 * np.pi / 86400)) # seconds_in_day_cos
    calculatedFeatures['dayIntoWeekSin'] = np.sin(dt.weekday() * (2 * np.pi / 7)) # day_in_week_sin
    calculatedFeatures['dayIntoWeekCos'] = np.cos(dt.weekday() * (2 * np.pi / 7)) # day_in_week_cos
    calculatedFeatures['dayIntoYearSin'] = np.sin(dayIntoYear * (2 * np.pi / 365)) # day_in_year_sin
    calculatedFeatures['dayIntoYearCos'] = np.cos(dayIntoYear * (2 * np.pi / 365)) # day_in_year_cos
    calculatedFeatures['volume'] = trade[1] # amount_traded
    calculatedFeatures['type'] = 1 if trade[4] == 'buy' else -1 # type

    return calculatedFeatures

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
    concatDf = pd.DataFrame.from_dict(data, orient='index', columns=features.COLUMNS)
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





