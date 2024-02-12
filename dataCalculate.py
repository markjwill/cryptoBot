import copy
import logging
import datetime
import numpy as np
import pandas as pd
import numpy as np
import tradePool


# update all trade[n] syntax to use named tradePool functions instead

def calculatePastPeriodFeatures(trades, milliseconds, features):
    if not trades:
        raise AssertionError(
            f'Empty trade list sent to past feature calculation.\n'
        )

    calculatedFeatures = copy.copy(features.PERIOD_FEATURES)

    # DTYPES = [
    #     ('price', np.float32),   # trade[0] 
    #     ('amount', np.float32),   # trade[1] 
    #     ('type', 'str'),        # trade[2] 
    #     ('date_ms', np.int64),     # trade[3] 
    #     ('trade_id', np.int64),     # trade[4] 
    #     ('coinbasePrice', np.float32),   # trades[5] 
    #     ('huobiPrice', np.float32),   # trades[6] 
    #     ('binancePrice', np.float32)    # trades[7] 
    # ]

    tradeArray = np.array(trades)

    indexes = features.FEATURE_INDEXES['exchange']

    # for i in range(2, features.LAST_TRADE_COUNT):
    #     tradeIndex = 0 - i
    #     calculatedFeatures[f'recent_{i}_price'] = \
    #         trades[tradeIndex][indexes['price']] - trades[-1][indexes['price']]
    #     calculatedFeatures[f'recent_{i}_volume'] = \
    #         trades[tradeIndex][indexes['volume']]
    #     calculatedFeatures[f'recent_{i}_time'] = \
    #         trades[tradeIndex][3] - trades[-1][3]


    firstTrade = trades[0]
    lastTrade = trades[-1]
    pivotPrice = {}

    calculatedFeatures['tradeCount'] = len(trades)

    for sourceName, index in features.FEATURE_INDEXES.items():
        logging.debug(index)
        pivotPrice[sourceName] = lastTrade[index['price']]

        if firstTrade[index['price']] == 0.0:
            raise AssertionError(
                f'Start price feature calculated as 0 at tradeId {firstTrade[4]}.'
            )
        if pivotPrice[sourceName] == 0.0:
            raise AssertionError(
                f'End price feature calculated as 0 at tradeId {lastTrade[4]}.'
            )

        if    index['price'] is not False and \
             index['volume'] is not False and \
               index['type'] is not False and \
           index['quantity'] is not False:

            calculatedFeatures[f'{sourceName}_highPrice'] = np.amax(tradeArray[:, index['price']], axis=0)
            calculatedFeatures[f'{sourceName}_lowPrice'] = np.amin(tradeArray[:, index['price']], axis=0)
            difference = np.diff(tradeArray[:, index['price']], axis=0)
            calculatedFeatures[f'{sourceName}_upVsDown'] = \
                np.sum(np.array(difference) > 0, axis=0) - np.sum(np.array(difference) < 0, axis=0)
            calculatedFeatures[f'{sourceName}_changeReal'] = \
                firstTrade[index['price']] - pivotPrice[sourceName]
            calculatedFeatures[f'{sourceName}_travelReal'] = \
                calculatedFeatures[f'{sourceName}_highPrice'] \
                - calculatedFeatures[f'{sourceName}_lowPrice']

            calculatedFeatures[f'{sourceName}_avgByTradesPrice'] = \
                ( np.array(tradeArray[:, index['price']]).sum() / calculatedFeatures['tradeCount'] ) \
                - pivotPrice[sourceName]
            calculatedFeatures[f'{sourceName}_lowPrice'] -= pivotPrice[sourceName]
            calculatedFeatures[f'{sourceName}_highPrice'] -= pivotPrice[sourceName]

            calculatedFeatures[f'{sourceName}_volume'] = np.sum(tradeArray[:, index['volume']])
            # calculatedFeatures[f'{sourceName}_buys'] = \
                # np.sum(tradeArray[:,index['type']] == 1.0, axis=0)
            # calculatedFeatures[f'{sourceName}_sells'] = \
                # abs(calculatedFeatures[f'{sourceName}_buys'] - calculatedFeatures['tradeCount'])
            calculatedFeatures[f'{sourceName}_buyVsSell'] = \
                calculatedFeatures[f'{sourceName}_buys'] - calculatedFeatures[f'{sourceName}_sells']
            calculatedFeatures[f'{sourceName}_buyVsSellVolume'] = \
                np.sum(np.multiply(tradeArray[:, index['volume']], tradeArray[:, index['type']]))
            calculatedFeatures[f'{sourceName}_avgByVolumePrice'] = \
                ( np.sum(np.multiply(tradeArray[:, index['volume']], tradeArray[:, index['price']])) \
                / calculatedFeatures[f'{sourceName}_volume'] ) - pivotPrice[sourceName]

        if sourceName != 'exchange':
            calculatedFeatures[f'{sourceName}_diffExchange'] = \
                pivotPrice[sourceName] - pivotPrice['exchange']


    logging.info('calculatedFeatures')
    logging.info(calculatedFeatures)

    return calculatedFeatures, pivotPrice['exchange']


def calculateNonPeriodFeatures(trade, features):
    calculatedFeatures = copy.copy(features.NON_PERIOD_FEATURES)
    dt = datetime.datetime.fromtimestamp(trade[3]/1000)
    t = dt.time()
    secondsIntoDay = (t.hour * 60 + t.minute) * 60 + t.second
    minutesIntoWeek = ( dt.weekday() * 1440 ) + ( secondsIntoDay / 60 )
    hourIntoMonth = float( dt.strftime("%d") * 24 ) + ( secondsIntoDay / 60 / 60 )
    # hourIntoYear = ( dt.timetuple().tm_yday * 24 ) + ( secondsIntoDay / 60 / 60 )
    calculatedFeatures['secondsIntoDaySin'] = np.sin(secondsIntoDay * (2 * np.pi / 86400))
    calculatedFeatures['secondsIntoDayCos'] = np.cos(secondsIntoDay * (2 * np.pi / 86400))
    calculatedFeatures['minutesIntoWeekSin'] = np.sin(minutesIntoWeek * (2 * np.pi / 10080))
    calculatedFeatures['minutesIntoWeekCos'] = np.cos(minutesIntoWeek * (2 * np.pi / 10080))
    calculatedFeatures['hoursIntoMonthSin'] = np.sin(hourIntoMonth * (2 * np.pi / 730.001))
    calculatedFeatures['hoursIntoMonthCos'] = np.cos(hourIntoMonth * (2 * np.pi / 730.001))
    # calculatedFeatures['hoursIntoYearSin'] = np.sin(hourIntoYear * (2 * np.pi / 8760))
    # calculatedFeatures['hoursIntoYearCos'] = np.cos(hourIntoYear * (2 * np.pi / 8760))
    calculatedFeatures['volume'] = trade[1] # amount_traded
    calculatedFeatures['type'] = trade[2] # type


    return calculatedFeatures

def calculateAllFeatureGroups(miniPool, features):
    # logging.debug(f'trade pool list len {len(tradePool.tradeList)}')
    pivotTrade = miniPool.getPivotTrade()
    tradeTimeMilliseconds = pivotTrade[3]
    pivotTradeId = pivotTrade[4]
    fileDestinations = {}
    fileDestinations['noNormalize'] = {}
    fileDestinations['normalize'] = {}

    nonPeriodFeatures = calculateNonPeriodFeatures(pivotTrade, features)
    fileDestinations = splitFeatures(fileDestinations, nonPeriodFeatures, features)
    del nonPeriodFeatures

    for timeName, periodMilliseconds in features.TIME_PERIODS.items():
        timeGroup = 'past'
        startTimeMilliseconds = tradeTimeMilliseconds - periodMilliseconds
        endTimeMilliseconds = tradeTimeMilliseconds
        pastFeatures, pivotPrice = calculatePastFeatureGroup(
                timeName, 
                miniPool, 
                startTimeMilliseconds, 
                pivotTradeId, 
                endTimeMilliseconds, 
                features )

        fileDestinations['normalize'] = fileDestinations['normalize'] \
            | pastFeatures
        del pastFeatures

        timeGroup = 'future'
        startTimeMilliseconds = tradeTimeMilliseconds 
        endTimeMilliseconds = tradeTimeMilliseconds + periodMilliseconds
        fileDestinations[timeName] = calculateFutureFeatureGroup(timeName, miniPool, pivotPrice)

    fileDestinations['normalize'] = fileDestinations['normalize'] \
            | miniPool.getLastNumberOfTrades()

    # fileDestinations['normalize'] = fileDestinations['normalize'] \
    #         | tradePool.getLastNumberOfTrades(tradePool.pivotIndex)

    logging.debug('All feature groups calculated')
    return fileDestinations

def calculateAllFeaturesToList(miniPool, features):
    fileDestinations = calculateAllFeatureGroups(miniPool, features)
    featuresDict = {}
    for value in fileDestinations.values():
        featuresDict = featuresDict | value

    sortedFeatures = {key: value for key, value in sorted(featuresDict.items())}
    featuresList = list(sortedFeatures.values())
    return featuresList

def calculatePastFeatureGroup(name, miniPool, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds, features):
    logging.debug(f'Collecting trades and calculating Group past_{name}')
    periodTrades = miniPool.getTradeList(f'past_{name}')
    pastFeatures, pivotPrice = calculatePastPeriodFeatures(periodTrades, endTimeMilliseconds - startTimeMilliseconds, features)
    pastFeatures = {f'{name}_{k}': v for k, v in pastFeatures.items()}

    return pastFeatures, pivotPrice

def calculateFuturePeriodFeatures(trades, pivotPrice):
    if not trades:
        raise AssertionError(
            f'Empty trade list sent to future feature calculation.\n'
        )

    calculatedFeatures = {
        'futurePrice' : trades[-1][0] - pivotPrice #end price
    }
    return calculatedFeatures

def calculateFutureFeatureGroup(name, miniPool, pivotPrice):
    logging.debug(f'Collecting trades and calculating Group future_{name}')
    periodTrades = miniPool.getFutureTrade(f'future_{name}')
    futureFeature = calculateFuturePeriodFeatures(periodTrades, pivotPrice)
    futureFeature = {f'{name}_{k}': v for k, v in futureFeature.items()}

    return futureFeature

def splitFeatures(fileDestinations, featureGroup, features):
    fileDestinations['noNormalize'] = fileDestinations['noNormalize'] \
        | dict([(key, value) for key, value \
        in featureGroup.items() \
        if key in features.DO_NOT_NORMALIZE])
    fileDestinations['normalize'] = fileDestinations['normalize'] \
        | dict([(key, value) for key, value in \
        featureGroup.items() \
        if key not in features.DO_NOT_NORMALIZE])
    return fileDestinations






