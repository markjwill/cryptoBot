import logging
import datetime
import os
import math
import timing
import memory_profiler

# trades[0] price
# trades[1] amount
# trades[2] type
# trades[3] date_ms
# trades[4] trade_id
# trades[5] coinbasePrice
# trades[6] huobiPrice
# trades[7] binancePrice

class TradePool():

    MILLISECONDS_GAP_TOLERATED = 120000
    tradeList = []
    features = False

    def __init__(self, poolType):
        self.maxIndex = 0
        self.subPools = {}
        self.pivotTradeId = 0
        self.futureTrades = {}
        self.workerId = 0
        self.isMiniPool = False
        if poolType == 'mini':
            self.isMiniPool = True
        self.maxIndex = len(TradePool.tradeList)
        logging.debug(f'New pool init, isMini: {self.isMiniPool} list len: {len(TradePool.tradeList)}')

    # def setInitalTrades(self, ascendingOrderTradeList):
    #     TradePool.tradeList = ascendingOrderTradeList
    #     self.maxIndex = len(TradePool.tradeList)

    # def getPivotTrade(self):
    #     if self.pivotTradeIndex == 0:
    #         raise AssertionError(
    #             'A pivot trade was requested, but none was set.'
    #         )
    #     return self.getTradeAt(self.pivotTradeIndex)

    def getSecondInPool(self, name):
        # if self.isMiniPool:
        #     return TradePool.tradeList[self.subPools[f'{self.workerId}_{name}']['startIndex']+1]
        return TradePool.tradeList[self.subPools[name]['startIndex']+1]

    def getFirstBeforePool(self, name):
        # if self.isMiniPool:
        #     return TradePool.tradeList[self.subPools[f'{self.workerId}_{name}']['startIndex']-1]
        return TradePool.tradeList[self.subPools[name]['startIndex']-1]

    def getFirstInPool(self, name=False):
        if not name:
            return TradePool.tradeList[0]
        # if self.isMiniPool:
        #     return TradePool.tradeList[self.subPools[f'{self.workerId}_{name}']['startIndex']]
        return TradePool.tradeList[self.subPools[name]['startIndex']]

    def getLastInPool(self, name=False):
        if not name:
            return TradePool.tradeList[-1]
        # if self.isMiniPool:
        #     return TradePool.tradeList[self.subPools[f'{self.workerId}_{name}']['endIndex']]
        return TradePool.tradeList[self.subPools[name]['endIndex']]

    def getPivotTrade(self):
        return self.getLastInPool(f'past_{self.features.MAX_PERIOD_NAME}')

    def getTradePrice(self, trade):
        return trade[0]

    def getTradeAmount(self, trade):
        return trade[1]

    def getTradeType(self, trade):
        return trade[2]

    def getTradeMilliseconds(self, trade):
        return trade[3]

    def getTradeId(self, trade):
        return trade[4]

    def getTradeAt(self, index):
        return TradePool.tradeList[index]

    def logTime(self, milliseconds):
        return datetime.datetime.fromtimestamp(milliseconds/1000.0).strftime('%Y-%m-%d %H:%M:%S')

    def getTradeList(self, name):
        startIndex = self.subPools[name]['startIndex']
        endIndex = self.subPools[name]['endIndex']
        # if self.isMiniPool:
        #     startIndex = self.subPools[f'{self.workerId}_{name}']['startIndex']
        #     endIndex = self.subPools[f'{self.workerId}_{name}']['endIndex']
        if endIndex == -1:
            return TradePool.tradeList[startIndex:]
        return TradePool.tradeList[startIndex:endIndex + 1]

    # def setStarterIndexes(self, starterPercents):
    #     for name, percent in starterPercents.items():
    #         count = math.ceil(self.maxIndex * percent)
    #         startIndex = max(self.maxIndex - count, 0)
    #         self.subPools[f'past_{name}'] = {
    #             'startIndex': startIndex,
    #             'endIndex': -1
    #         }

    def addPool(self, name):
        if name not in self.subPools:
            logging.debug(f'Adding index tracking for subPool: {name}')
            self.subPools[name] = {
                'startIndex': 0,
                'endIndex': -1
            }

    def logPoolDetails(self):
        if self.maxIndex == 0:
            logging.warning('Trade pool is empty')
            return

        logging.debug(f'Pool max index {self.maxIndex}')
        poolStartTime = self.logTime(self.getTradeMilliseconds(self.getFirstInPool()))
        poolEndTime = self.logTime(self.getTradeMilliseconds(self.getLastInPool()))
        logging.debug(f'Pool startTime: {poolStartTime} endTime: {poolEndTime}')
        if logging.DEBUG == logging.getLogger().getEffectiveLevel():
            for name, indexes in self.subPools.items():
                logging.debug(f'{name} startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')
                subPoolStartTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]['startIndex'])))
                subPoolEndTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]['endIndex'])))
                logging.debug(f'{name} startTime: {subPoolStartTime} endTime: {subPoolEndTime}')
        logging.debug('logPoolDetails complete.')

    # def miniPoolDataGaps(self):
    #     logging.debug('Starting data gap check')
    #     previousTimeMilliseconds = self.getTradeMilliseconds(self.getFirstInPool())
    #     for trade in TradePool.tradeList:
    #         # logging.debug(f"Gap compare {self.logTime(self.getTradeMilliseconds(trade) - self.MILLISECONDS_GAP_TOLERATED) } > {self.logTime(previousTimeMilliseconds)}")
    #         if ( self.getTradeMilliseconds(trade) - self.MILLISECONDS_GAP_TOLERATED ) > previousTimeMilliseconds:
    #             return True
    #         previousTimeMilliseconds = self.getTradeMilliseconds(trade)
    #     logging.debug('Finished data gap check')
    #     return False

    def mapGapIterable(self):
        logging.debug('Starting gap mapping')
        previousTimeMilliseconds = 0
        gapStartMillieseconds = self.getTradeMilliseconds(self.getFirstInPool())
        poolEndMillieseconds = \
            self.getTradeMilliseconds(self.getLastInPool()) - TradePool.features.MAX_PERIOD
        inGap = True
        # gapStartIndex = 0
        nonGapIndexes = []
        for index in range(len(TradePool.tradeList)):
            trade = self.getTradeAt(index)
            tradeMilliseconds = self.getTradeMilliseconds(trade)

            if inGap:
                if tradeMilliseconds < gapStartMillieseconds + TradePool.features.MAX_PERIOD:
                    previousTimeMilliseconds = tradeMilliseconds
                    continue
                else:
                    logging.debug(f'Gap ended {index}')
                    inGap = False

            if ( tradeMilliseconds - self.MILLISECONDS_GAP_TOLERATED ) > previousTimeMilliseconds:
                logging.debug(f'Gap started {index}')
                logging.debug(f'( {tradeMilliseconds} - {self.MILLISECONDS_GAP_TOLERATED} ) > {previousTimeMilliseconds}')

                gapStartMillieseconds = self.getTradeMilliseconds(self.getTradeAt(index - 1))
                nonGapIndexes.pop()
                inGap = True

            if tradeMilliseconds > poolEndMillieseconds:
                nonGapIndexes.pop()
                break

            if not inGap:
                nonGapIndexes.append(index)

            previousTimeMilliseconds = tradeMilliseconds
            # logging.debug(f'previousTimeMilliseconds set 3 {previousTimeMilliseconds}')

        logging.debug('Finished gap mapping')
        # logging.debug(nonGapIndexes)
        # for startIndex, endIndex in gapIndexMap.items():
        #     logging.debug(f'Gap from {self.logTime(self.getTradeMilliseconds(self.getTradeAt(startIndex)))}')
        #     logging.debug(f'To {self.logTime(self.getTradeMilliseconds(self.getTradeAt(endIndex)))}')

        return tuple(nonGapIndexes)

    # def mapGaps(self):
    #     logging.debug('Starting gap mapping')
    #     previousTimeMilliseconds = 0
    #     gapStartMillieseconds = self.getTradeMilliseconds(self.getFirstInPool())
    #     inGap = True
    #     gapStartIndex = 0
    #     gapIndexMap = { gapStartIndex:0 }
    #     for index in range(len(TradePool.tradeList)):
    #         trade = self.getTradeAt(index)
    #         tradeMilliseconds = self.getTradeMilliseconds(trade)

    #         if inGap:
    #             if tradeMilliseconds < gapStartMillieseconds + TradePool.features.MAX_PERIOD:
    #                 # logging.debug(f'{tradeMilliseconds} < {gapStartMillieseconds} + {TradePool.features.MAX_PERIOD}')
    #                 gapIndexMap[gapStartIndex] = index
    #                 previousTimeMilliseconds = tradeMilliseconds
    #                 # logging.debug(f'previousTimeMilliseconds set 1 {previousTimeMilliseconds}')
    #                 # logging.debug('Continueing 1')
    #                 continue
    #             else:
    #                 logging.debug(f'Gap ended {index}')
    #                 inGap = False

    #         if ( tradeMilliseconds - self.MILLISECONDS_GAP_TOLERATED ) > previousTimeMilliseconds:
    #             logging.debug(f'Gap found {gapStartIndex} {index}')
    #             logging.debug(gapIndexMap)
    #             logging.debug(f'( {tradeMilliseconds} - {self.MILLISECONDS_GAP_TOLERATED} ) > {previousTimeMilliseconds}')
    #             previousIndex = index - 1
    #             while (tradeMilliseconds - TradePool.features.MAX_PERIOD > \
    #                     self.getTradeMilliseconds(self.getTradeAt(previousIndex))):
    #                 previousIndex -= 1
    #                 if previousIndex <= gapStartIndex:
    #                     gapIndexMap[gapStartIndex] = index
    #                     inGap = True
    #                     logging.debug(f'Gap intersects with previous gap {gapStartIndex} {index}')
    #                     logging.debug('Continueing 3')
    #                     previousTimeMilliseconds = tradeMilliseconds
    #                     logging.debug(f'previousTimeMilliseconds set 2 {previousTimeMilliseconds}')
    #                     continue

    #             gapStartIndex = previousIndex
    #             gapStartMillieseconds = self.getTradeMilliseconds(self.getTradeAt(previousIndex))
    #             gapIndexMap[previousIndex] = previousIndex + 1
    #             inGap = True

    #         previousTimeMilliseconds = tradeMilliseconds
    #         # logging.debug(f'previousTimeMilliseconds set 3 {previousTimeMilliseconds}')

    #     logging.debug('Finished gap mapping')
    #     logging.debug(gapIndexMap)
    #     for startIndex, endIndex in gapIndexMap.items():
    #         logging.debug(f'Gap from {self.logTime(self.getTradeMilliseconds(self.getTradeAt(startIndex)))}')
    #         logging.debug(f'To {self.logTime(self.getTradeMilliseconds(self.getTradeAt(endIndex)))}')

    #     return gapIndexMap

    # def dataGaps(self):
    #     logging.debug('Starting data gap check')
    #     previousTimeMilliseconds = self.getTradeMilliseconds(self.getFirstInPool())
    #     for trade in TradePool.tradeList:
    #         # logging.debug(f"Gap compare {self.logTime(self.getTradeMilliseconds(trade) - self.MILLISECONDS_GAP_TOLERATED) } > {self.logTime(previousTimeMilliseconds)}")
    #         if ( self.getTradeMilliseconds(trade) - self.MILLISECONDS_GAP_TOLERATED ) > previousTimeMilliseconds:
    #             return True
    #         previousTimeMilliseconds = self.getTradeMilliseconds(trade)
    #     logging.debug('Finished data gap check')
    #     return False

    # def rotateTradesIntoTheFuture(self, newTrades):
    #     if self.maxIndex == 0:
    #         return self.setInitalTrades(newTrades)
    #     if n := len(newTrades):
    #         del TradePool.tradeList[:n]
    #         TradePool.tradeList = TradePool.tradeList + newTrades
    #         self.maxIndex = len(TradePool.tradeList)
    #         for name, indexes in self.subPools.items():
    #             indexes['startIndex'] = 0
    #             indexes['endIndex'] = -1
    #         return
    #     raise AssertionError(
    #         'An empty set of trades was recieved when trying to add more trades to a pool.'
    #     )

    def startIndexExistsCheck(self, listIndex, name, debug):
        if listIndex <= -1:
            logging.error(
                f'For {name}, a trade farther in the past than the set of trades\n'
                f'in the pool was requested by a start index at {listIndex}.\n'
                'Calculation must always traverse trades from the past to the\n'
                f'future. function debug: {debug}'
            )
            os._exit(0)
        if listIndex >= self.maxIndex:
            logging.error(
                f'For {name}, a trade farther in the future than the set of trades\n'
                f'in the pool was requested by a start index at {listIndex}.\n'
                f'function debug: {debug}'
            )
            os._exit(0)

    def endIndexExistsCheck(self, listIndex, name, debug):
        if listIndex >= 0:
            logging.error(
                f'For {name}, a trade farther in the future than the set of trades\n'
                f'in the pool was requested by an end index at {listIndex}.\n'
                f'function debug: {debug}'
            )
            os._exit(0)
        if listIndex < self.maxIndex * -1:
            logging.error(
                f'For {name}, a trade farther in the past than the set of trades\n'
                f'in the pool was requested by an end index at {listIndex}.\n'
                'Calculation must always traverse trades from the past to the future\n'
                f'function debug: {debug}'
            )
            os._exit(0)

    def isMillisecondsInPool(self, targetMilliseconds, name, debug):
        poolStartMilliseconds = self.getTradeMilliseconds(self.getFirstInPool())
        if poolStartMilliseconds > targetMilliseconds:
            tradeDatetime = self.logTime(targetMilliseconds)
            poolStartDatetime = self.logTime(poolStartMilliseconds)
            logging.error(
                f'Trade requested at {tradeDatetime}  Pool start at {poolStartDatetime}'
                f'For {name}, a trade farther in the past than the set of trades in the pool was requested by milliseconds.  '
                'Calculation must always traverse trades from the past to the future. '
                f'function debug: {debug}'
            )
            os._exit(0)
        poolEndMilliseconds = self.getTradeMilliseconds(self.getLastInPool())
        if poolEndMilliseconds < targetMilliseconds:
            tradeDatetime = self.logTime(targetMilliseconds)
            poolEndDatetime = self.logTime(poolEndMilliseconds)
            logging.error(
                f'Trade requested at {tradeDatetime}  Pool end at {poolEndDatetime}\n'
                f'For {name}, a trade farther in the future than the set of trades\n'
                f'in the pool was requested by milliseconds.\n'
                f'function debug: {debug}'
            )
            os._exit(0)

    # @profile
    def getMiniPool(self, pivotIndex, miniPool, workerId):
        # processStepStart = timing.startCalculation()
        # logging.info('getMiniPoolStart')
        # timing.progressCalculation(processStepStart)

        miniPool.workerId = workerId
        miniPool.isMiniPool = True
        miniPool.features = self.features
        pivotTrade = self.getTradeAt(pivotIndex)
        pivotTimeMilliseconds = self.getTradeMilliseconds(pivotTrade)
        endTimeMilliseconds = pivotTimeMilliseconds

        logging.debug(f'index: {pivotIndex} id: {pivotTrade[4]}')

        for timeName, periodMilliseconds in TradePool.features.TIME_PERIODS.items():
            startTimeMilliseconds = pivotTimeMilliseconds - periodMilliseconds
            self.selectMultipleTrades(f'{miniPool.workerId}_past_{timeName}', startTimeMilliseconds, pivotIndex)

        logging.debug(self.subPools.items())

        for name, indexes in self.subPools.items():
            if name.startswith(f'{miniPool.workerId}_past_'):
                miniPool.subPools[name] = {}
                miniPool.subPools[name]['startIndex'] = self.subPools[f'{name}']['startIndex']
                miniPool.subPools[name]['endIndex'] = pivotIndex

        # miniPool.setInitalTrades(self.getTradeList(f'past_{TradePool.features.MAX_PERIOD_NAME}').copy())

        for timeName, periodMilliseconds in TradePool.features.TIME_PERIODS.items():
            name = f'future_{timeName}'
            targetMilliseconds = pivotTimeMilliseconds + periodMilliseconds
            tradeItem = self.selectFutureTrade(name, targetMilliseconds)
            miniPool.futureTrades[name] = tradeItem

        miniPool.logPoolDetails()

        return miniPool

    # def transferTradeIndexes(self, taretPool, sourceSubPoolName):
    #     targetPoolStart = self.subPools[sourceSubPoolName]['startIndex']
    #     for timeName, periodMilliseconds in TradePool.features.TIME_PERIODS.items():
    #         if timeName not in targetPool.subPools:
    #             targetPool.addPool(timeName)
    #         targetPool.selectMultipleTrades(name, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds)


    # def getTrades(self, name, timeGroup, pivotTradeId, startTimeMilliseconds, endTimeMilliseconds):
    #     if name not in self.subPools:
    #         self.addPool(name)
    #     logging.debug(f'Inital startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')
    #     logging.debug(f'Moving Indexs for subPool: {name}')
    #     if timeGroup == 'future':
    #         return self.selectFutureTrade(name, endTimeMilliseconds)
    #     self.selectMultipleTrades(name, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds)
    #     pastTrades = self.getTradeList(name)
    #     logging.debug(f'Getting {len(pastTrades)} trades for {name} {timeGroup} at tradeId {pivotTradeId}')
    #     return pastTrades

    def getFutureTrade(self, name):
        return self.futureTrades[name]

    def selectFutureTrade(self, name, targetMilliseconds):
        if f'{self.workerId}_{name}' in self.futureTrades:
            return self.futureTrades[f'{self.workerId}_{name}']
        if f'{self.workerId}_{name}' not in self.subPools:
            self.addPool(f'{self.workerId}_{name}')

        initalStartTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[f'{self.workerId}_{name}']['startIndex'])))
        targetStartTime = self.logTime(targetMilliseconds)
        logging.debug(f'Inital startTime: {initalStartTime} Target startTime: {targetStartTime}')

        if self.getTradeMilliseconds(self.getFirstBeforePool(f'{self.workerId}_{name}')) > targetMilliseconds:
            while self.getTradeMilliseconds(self.getFirstInPool(f'{self.workerId}_{name}')) > targetMilliseconds:
                logging.debug('This should never happen!!')
                self.subPools[f'{self.workerId}_{name}']['startIndex'] -= 1
                # self.startIndexExistsCheck(self.subPools[f'{self.workerId}_{name}']['startIndex'], f'{self.workerId}_{name}', 'subset target trade > target time')

        while self.getTradeMilliseconds(self.getFirstInPool(f'{self.workerId}_{name}')) < targetMilliseconds:
            self.subPools[f'{self.workerId}_{name}']['startIndex'] += 1
            # self.startIndexExistsCheck(self.subPools[f'{self.workerId}_{name}']['startIndex'], f'{self.workerId}_{name}', 'subset target trade < target time')

        self.subPools[f'{self.workerId}_{name}']['endIndex'] = self.subPools[f'{self.workerId}_{name}']['startIndex']

        logging.debug(f'Final startIndex: {self.subPools[name]["startIndex"]}')

        return self.getTradeList(f'{self.workerId}_{name}')


    def selectMultipleTrades(self, name, startTimeMilliseconds, pivotIndex):
        if self.isMiniPool:
            logging.error(
                'Trade selection attempt in miniPool\n'
                'All trade selection must be done in parent pool.'
            )
            os._exit(0)
        if name not in self.subPools:
            self.addPool(name)
        # if not self.isMiniPool:
        #     self.isMillisecondsInPool(startTimeMilliseconds, name, 'start target time > pool start time and < pool end time')
        #     self.isMillisecondsInPool(endTimeMilliseconds, name, 'end taret time > pool start time and < pool end time')
        # self.startIndexExistsCheck(self.subPools[name]['startIndex'], name, 'inital subpool start index check')
        # self.endIndexExistsCheck(self.subPools[name]['endIndex'], name, 'inital subpool end index check')

        initalStartTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]['startIndex'])))
        # initalEndTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]['endIndex'])))
        logging.debug(f'Inital startTime: {initalStartTime}')
        targetStartTime = self.logTime(startTimeMilliseconds)
        # targetEndTime = self.logTime(endTimeMilliseconds)
        logging.debug(f'Target startTime: {targetStartTime}')

        if self.getTradeMilliseconds(self.getFirstBeforePool(name)) > startTimeMilliseconds:
            while self.getTradeMilliseconds(self.getFirstInPool(name)) > startTimeMilliseconds:
                self.subPools[name]['startIndex'] -= 1
                logging.debug('This should never happen!!')
                # self.startIndexExistsCheck(self.subPools[name]['startIndex'], name, 'subset first trade > start time')

        if self.getTradeMilliseconds(self.getSecondInPool(name)) < startTimeMilliseconds:
            while self.getTradeMilliseconds(self.getFirstInPool(name)) < startTimeMilliseconds:
                self.subPools[name]['startIndex'] += 1
                # self.startIndexExistsCheck(self.subPools[name]['startIndex'], name, 'subset first trade < start time')

        self.subPools[name]['endIndex'] = pivotIndex

        # while self.getTradeMilliseconds(self.getLastInPool(name)) > endTimeMilliseconds:
        #     self.subPools[name]['endIndex'] -= 1
        #     self.endIndexExistsCheck(self.subPools[name]['endIndex'], name, 'subset last trade > end time')

        # while self.getTradeMilliseconds(self.getLastInPool(name)) < endTimeMilliseconds:
        #     self.subPools[name]['endIndex'] += 1
        #     self.endIndexExistsCheck(self.subPools[name]['endIndex'], name, 'subset last trade < end time')
        #     if pivotTradeId == self.getTradeId(self.getLastInPool(name)):
        #         break

        # logging.debug(f'pivotTradeId: {pivotTradeId} lastTradeId: {self.getTradeId(self.getLastInPool(name))} endIndex: {self.subPools[name]["endIndex"]}')
        # while pivotTradeId != self.getTradeId(self.getLastInPool(name)):
        #     self.subPools[name]['endIndex'] += 1
        #     self.endIndexExistsCheck(self.subPools[name]['endIndex'], name, 'subset last trade id != pivotTradeId')

        logging.debug(f'Final startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')



