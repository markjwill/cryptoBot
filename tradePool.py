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

class TradePool:

    MILLISECONDS_GAP_TOLERATED = 120000

    def __init__(self):
        self.tradeList = []
        self.maxIndex = 0
        self.subPools = {}
        self.pivotTradeId = 0
        self.futureTrades = {}
        self.isMiniPool = False

    def setInitalTrades(self, ascendingOrderTradeList):
        self.tradeList = ascendingOrderTradeList
        self.maxIndex = len(self.tradeList)

    # def getPivotTrade(self):
    #     if self.pivotTradeIndex == 0:
    #         raise AssertionError(
    #             'A pivot trade was requested, but none was set.'
    #         )
    #     return self.getTradeAt(self.pivotTradeIndex)

    def getFirstInPool(self, name=False):
        if not name:
            return self.tradeList[0]
        return self.tradeList[self.subPools[name]['startIndex']]

    def getLastInPool(self, name=False):
        if not name:
            return self.tradeList[-1]
        return self.tradeList[self.subPools[name]['endIndex']]

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
        return self.tradeList[index]

    def logTime(self, milliseconds):
        return datetime.datetime.fromtimestamp(milliseconds/1000.0).strftime('%Y-%m-%d %H:%M:%S')

    def getTradeList(self, name):
        startIndex = self.subPools[name]['startIndex']
        endIndex = self.subPools[name]['endIndex']
        if endIndex == -1:
            return self.tradeList[startIndex:]
        return self.tradeList[startIndex:endIndex + 1]

    def setStarterIndexes(self, starterPercents):
        for name, percent in starterPercents.items():
            count = math.ceil(self.maxIndex * percent)
            startIndex = max(self.maxIndex - count, 0)
            self.subPools[f'past_{name}'] = {
                'startIndex': startIndex,
                'endIndex': -1
            }

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
    #     for trade in self.tradeList:
    #         # logging.debug(f"Gap compare {self.logTime(self.getTradeMilliseconds(trade) - self.MILLISECONDS_GAP_TOLERATED) } > {self.logTime(previousTimeMilliseconds)}")
    #         if ( self.getTradeMilliseconds(trade) - self.MILLISECONDS_GAP_TOLERATED ) > previousTimeMilliseconds:
    #             return True
    #         previousTimeMilliseconds = self.getTradeMilliseconds(trade)
    #     logging.debug('Finished data gap check')
    #     return False

    def mapGaps(self, features):
        logging.debug('Starting gap mapping')
        previousTimeMilliseconds = 0
        gapStartMillieseconds = self.getTradeMilliseconds(self.getFirstInPool())
        inGap = True
        gapStartIndex = 0
        gapIndexMap = { gapStartIndex:0 }
        for index in range(len(self.tradeList)):
            trade = self.getTradeAt(index)
            tradeMilliseconds = self.getTradeMilliseconds(trade)

            if inGap:
                if tradeMilliseconds < gapStartMillieseconds + features.MAX_PERIOD:
                    # logging.debug(f'{tradeMilliseconds} < {gapStartMillieseconds} + {features.MAX_PERIOD}')
                    gapIndexMap[gapStartIndex] = index
                    previousTimeMilliseconds = tradeMilliseconds
                    # logging.debug(f'previousTimeMilliseconds set 1 {previousTimeMilliseconds}')
                    # logging.debug('Continueing 1')
                    continue
                else:
                    logging.debug(f'Gap ended {index}')
                    inGap = False

            if ( tradeMilliseconds - self.MILLISECONDS_GAP_TOLERATED ) > previousTimeMilliseconds:
                logging.debug(f'Gap found {gapStartIndex} {index}')
                logging.debug(gapIndexMap)
                logging.debug(f'( {tradeMilliseconds} - {self.MILLISECONDS_GAP_TOLERATED} ) > {previousTimeMilliseconds}')
                previousIndex = index - 1
                while (tradeMilliseconds - features.MAX_PERIOD > \
                        self.getTradeMilliseconds(self.getTradeAt(previousIndex))):
                    previousIndex -= 1
                    if previousIndex <= gapStartIndex:
                        gapIndexMap[gapStartIndex] = index
                        inGap = True
                        logging.debug(f'Gap intersects with previous gap {gapStartIndex} {index}')
                        logging.debug('Continueing 3')
                        previousTimeMilliseconds = tradeMilliseconds
                        logging.debug(f'previousTimeMilliseconds set 2 {previousTimeMilliseconds}')
                        continue

                gapStartIndex = previousIndex
                gapStartMillieseconds = self.getTradeMilliseconds(self.getTradeAt(previousIndex))
                gapIndexMap[previousIndex] = previousIndex + 1
                inGap = True

            previousTimeMilliseconds = tradeMilliseconds
            # logging.debug(f'previousTimeMilliseconds set 3 {previousTimeMilliseconds}')

        logging.debug('Finished gap mapping')
        logging.debug(gapIndexMap)
        for startIndex, endIndex in gapIndexMap.items():
            logging.debug(f'Gap from {self.logTime(self.getTradeMilliseconds(self.getTradeAt(startIndex)))}')
            logging.debug(f'To {self.logTime(self.getTradeMilliseconds(self.getTradeAt(endIndex)))}')

        return gapIndexMap

    # def dataGaps(self):
    #     logging.debug('Starting data gap check')
    #     previousTimeMilliseconds = self.getTradeMilliseconds(self.getFirstInPool())
    #     for trade in self.tradeList:
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
    #         del self.tradeList[:n]
    #         self.tradeList = self.tradeList + newTrades
    #         self.maxIndex = len(self.tradeList)
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
    def getMiniPool(self, pivotIndex, features):
        # processStepStart = timing.startCalculation()
        # logging.info('getMiniPoolStart')
        # timing.progressCalculation(processStepStart)
        
        miniPool = TradePool()
        miniPool.isMiniPool = True
        pivotTrade = self.getTradeAt(pivotIndex)
        pivotTimeMilliseconds = self.getTradeMilliseconds(pivotTrade)
        endTimeMilliseconds = pivotTimeMilliseconds

        for timeName, periodMilliseconds in features.TIME_PERIODS.items():
            startTimeMilliseconds = pivotTimeMilliseconds - periodMilliseconds
            self.selectMultipleTrades(f'past_{timeName}', startTimeMilliseconds, self.getTradeId(pivotTrade), endTimeMilliseconds)

        toStartIndex = self.subPools[f'past_{features.MAX_PERIOD_NAME}']['startIndex']
        for name, indexes in self.subPools.items():
            if name.startswith('past_'):
                miniPool.subPools[name] = {}
                miniPool.subPools[name]['startIndex'] = \
                    self.subPools[name]['startIndex'] - toStartIndex

                miniPool.subPools[name]['endIndex'] = -1

        miniPool.setInitalTrades(self.getTradeList(f'past_{features.MAX_PERIOD_NAME}').copy())

        for timeName, periodMilliseconds in features.TIME_PERIODS.items():
            name = f'future_{timeName}'
            targetMilliseconds = pivotTimeMilliseconds + periodMilliseconds
            tradeItem = self.selectFutureTrade(name, targetMilliseconds)
            miniPool.futureTrades[name] = tradeItem

        miniPool.logPoolDetails()

        return miniPool

    # def transferTradeIndexes(self, taretPool, sourceSubPoolName):
    #     targetPoolStart = self.subPools[sourceSubPoolName]['startIndex']
    #     for timeName, periodMilliseconds in features.TIME_PERIODS.items():
    #         if timeName not in targetPool.subPools:
    #             targetPool.addPool(timeName)
    #         targetPool.selectMultipleTrades(name, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds)


    def getTrades(self, name, timeGroup, pivotTradeId, startTimeMilliseconds, endTimeMilliseconds):
        if name not in self.subPools:
            self.addPool(name)
        logging.debug(f'Inital startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')
        logging.debug(f'Moving Indexs for subPool: {name}')
        if timeGroup == 'future':
            return self.selectFutureTrade(name, endTimeMilliseconds)
        self.selectMultipleTrades(name, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds)
        pastTrades = self.getTradeList(name)
        logging.debug(f'Getting {len(pastTrades)} trades for {name} {timeGroup} at tradeId {pivotTradeId}')
        return pastTrades

    def getFutureTrade(self, name):
        return self.futureTrades[name]

    def selectFutureTrade(self, name, targetMilliseconds):
        if name in self.futureTrades:
            return self.futureTrades[name]
        if name not in self.subPools:
            self.addPool(name)
        self.subPools[name]['endIndex'] = self.subPools[name]['startIndex']

        initalStartTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]['startIndex'])))
        targetStartTime = self.logTime(targetMilliseconds)
        logging.debug(f'Inital startTime: {initalStartTime} Target startTime: {targetStartTime}')

        while self.getTradeMilliseconds(self.getFirstInPool(name)) < targetMilliseconds:
            self.subPools[name]['startIndex'] += 1
            self.subPools[name]['endIndex'] += 1
            self.startIndexExistsCheck(self.subPools[name]['startIndex'], name, 'subset target trade < target time')

        while self.getTradeMilliseconds(self.getFirstInPool(name)) > targetMilliseconds:
            self.subPools[name]['startIndex'] -= 1
            self.subPools[name]['endIndex'] -= 1
            self.startIndexExistsCheck(self.subPools[name]['startIndex'], name, 'subset target trade > target time')

        logging.debug(f'Final startIndex: {self.subPools[name]["startIndex"]}')

        return self.getTradeList(name)


    def selectMultipleTrades(self, name, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds):
        if self.isMiniPool:
            logging.error(
                'Trade selection attempt in miniPool.\n'
                'All trade selection must be don in parent pool.'
            )
            os._exit(0)
        if name not in self.subPools:
            self.addPool(name)
        if not self.isMiniPool:
            self.isMillisecondsInPool(startTimeMilliseconds, name, 'start target time > pool start time and < pool end time')
            self.isMillisecondsInPool(endTimeMilliseconds, name, 'end taret time > pool start time and < pool end time')
        self.startIndexExistsCheck(self.subPools[name]['startIndex'], name, 'inital subpool start index check')
        self.endIndexExistsCheck(self.subPools[name]['endIndex'], name, 'inital subpool end index check')

        initalStartTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]['startIndex'])))
        initalEndTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]['endIndex'])))
        logging.debug(f'Inital startTime: {initalStartTime} endTime: {initalEndTime}')
        targetStartTime = self.logTime(startTimeMilliseconds)
        targetEndTime = self.logTime(endTimeMilliseconds)
        logging.debug(f'Target startTime: {targetStartTime} endTime: {targetEndTime}')

        while self.getTradeMilliseconds(self.getFirstInPool(name)) > startTimeMilliseconds:
            self.subPools[name]['startIndex'] -= 1
            self.startIndexExistsCheck(self.subPools[name]['startIndex'], name, 'subset first trade > start time')

        while self.getTradeMilliseconds(self.getFirstInPool(name)) < startTimeMilliseconds:
            self.subPools[name]['startIndex'] += 1
            self.startIndexExistsCheck(self.subPools[name]['startIndex'], name, 'subset first trade < start time')

        while self.getTradeMilliseconds(self.getLastInPool(name)) > endTimeMilliseconds:
            self.subPools[name]['endIndex'] -= 1
            self.endIndexExistsCheck(self.subPools[name]['endIndex'], name, 'subset last trade > end time')

        while self.getTradeMilliseconds(self.getLastInPool(name)) < endTimeMilliseconds:
            self.subPools[name]['endIndex'] += 1
            self.endIndexExistsCheck(self.subPools[name]['endIndex'], name, 'subset last trade < end time')
            if pivotTradeId == self.getTradeId(self.getLastInPool(name)):
                break

        logging.debug(f'pivotTradeId: {pivotTradeId} lastTradeId: {self.getTradeId(self.getLastInPool(name))} endIndex: {self.subPools[name]["endIndex"]}')
        while pivotTradeId != self.getTradeId(self.getLastInPool(name)):
            self.subPools[name]['endIndex'] += 1
            self.endIndexExistsCheck(self.subPools[name]['endIndex'], name, 'subset last trade id != pivotTradeId')

        logging.debug(f'Final startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')



