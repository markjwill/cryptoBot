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

    MILLISECONDS_GAP_TOLERATED = 100000
    tradeList = []
    features = False
    pivotIndex = 0

    def __init__(self, poolType):
        self.maxIndex = 0
        self.subPools = {}
        self.pivotTradeId = 0
        self.futureTrades = {}
        self.workerId = 0
        self.isMiniPool = False
        self.pivotIndex = 0
        if poolType == 'mini':
            self.isMiniPool = True
        self.maxIndex = len(TradePool.tradeList)
        logging.debug(f'x{self.workerId} New pool init, isMini: {self.isMiniPool} list len: {len(TradePool.tradeList)}')

    def getSecondInPool(self, name):
        secondIn = TradePool.tradeList[self.subPools[name]['startIndex']+1]
        lastIn = TradePool.tradeList[self.subPools[name]['endIndex']]
        if secondIn > lastIn:
            return lastIn
        return secondIn

    def getFirstBeforePool(self, name):
        return TradePool.tradeList[self.subPools[name]['startIndex']-1]

    def getFirstInPool(self, name=False):
        if not name:
            return TradePool.tradeList[0]
        return TradePool.tradeList[self.subPools[name]['startIndex']]

    def getLastInPool(self, name=False):
        if not name:
            return TradePool.tradeList[-1]
        return TradePool.tradeList[self.subPools[name]['endIndex']]

    def getSecondToLastInPool(self, name):
        secondToLast = TradePool.tradeList[self.subPools[name]['endIndex'] - 1]
        firstIn = TradePool.tradeList[self.subPools[name]['startIndex']]
        if secondToLast < firstIn:
            return firstIn
        return secondToLast

    def getFirstAfterPool(self, name):
        return TradePool.tradeList[self.subPools[name]['endIndex'] + 1]

    def getPivotTrade(self):
        return self.getLastInPool(f'past_{TradePool.features.MAX_PERIOD_NAME}')

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
        if endIndex == -1:
            return TradePool.tradeList[startIndex:]
        return TradePool.tradeList[startIndex:endIndex + 1]


    def addPool(self, name, futurePool=False):
        if name not in self.subPools:
            logging.debug(f'x{self.workerId} Adding index tracking for subPool: {name}')
            self.subPools[name] = {
                'startIndex': 0,
                'endIndex': -1
            }
            if futurePool:
                self.subPools[name]['endIndex'] = 1

    def logPoolDetails(self):
        if self.maxIndex == 0:
            logging.error(f'x{self.workerId} Trade pool is empty')
            return

        if self.isMiniPool:
            logging.debug(f'x{self.workerId} miniPool pivot index: {self.pivotIndex}')

        logging.debug(f'Pool max index {self.maxIndex}')
        poolStartTime = self.logTime(self.getTradeMilliseconds(self.getFirstInPool()))
        poolEndTime = self.logTime(self.getTradeMilliseconds(self.getLastInPool()))
        logging.debug(f'Pool startTime: {poolStartTime} endTime: {poolEndTime}')
        if logging.DEBUG == logging.getLogger().getEffectiveLevel():
            for name, indexes in self.subPools.items():
                logging.debug(f'x{self.workerId} {name} startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')
                subPoolStartTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]['startIndex'])))
                subPoolEndTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]['endIndex'])))
                logging.debug(f'x{self.workerId} {name} startTime: {subPoolStartTime} endTime: {subPoolEndTime}')
        logging.debug('logPoolDetails complete.')

    def mapGapIterable(self):
        logging.debug(f'x{self.workerId} Starting gap mapping')
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
                    logging.debug(f'x{self.workerId} Gap ended {index}')
                    inGap = False

            if ( tradeMilliseconds - self.MILLISECONDS_GAP_TOLERATED ) > previousTimeMilliseconds:
                logging.debug(f'x{self.workerId} Gap started {index}')
                logging.debug(f'x{self.workerId} ( {tradeMilliseconds} - {self.MILLISECONDS_GAP_TOLERATED} ) > {previousTimeMilliseconds}')

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

    def startIndexExistsCheck(self, listIndex, name, debug):
        if listIndex <= -1:
            logging.error(
                f'x{self.workerId} For {name}, a trade farther in the past than the set of trades\n'
                f'x{self.workerId} in the pool was requested by a start index at {listIndex}.\n'
                f'x{self.workerId} Calculation must always traverse trades from the past to the\n'
                f'x{self.workerId} future. function debug: {debug}'
            )
            os._exit(0)
        if listIndex >= self.maxIndex:
            logging.error(
                f'x{self.workerId} For {name}, a trade farther in the future than the set of trades\n'
                f'x{self.workerId} in the pool was requested by a start index at {listIndex}.\n'
                f'x{self.workerId} function debug: {debug}'
            )
            os._exit(0)

    def endIndexExistsCheck(self, listIndex, name, debug):
        if listIndex >= 0:
            logging.error(
                f'x{self.workerId} For {name}, a trade farther in the future than the set of trades\n'
                f'x{self.workerId} in the pool was requested by an end index at {listIndex}.\n'
                f'x{self.workerId} function debug: {debug}'
            )
            os._exit(0)
        if listIndex < self.maxIndex * -1:
            logging.error(
                f'x{self.workerId} For {name}, a trade farther in the past than the set of trades\n'
                f'x{self.workerId} in the pool was requested by an end index at {listIndex}.\n'
                f'x{self.workerId} Calculation must always traverse trades from the past to the future\n'
                f'x{self.workerId} function debug: {debug}'
            )
            os._exit(0)

    def isMillisecondsInPool(self, targetMilliseconds, name, debug):
        poolStartMilliseconds = self.getTradeMilliseconds(self.getFirstInPool())
        if poolStartMilliseconds > targetMilliseconds:
            tradeDatetime = self.logTime(targetMilliseconds)
            poolStartDatetime = self.logTime(poolStartMilliseconds)
            logging.error(
                f'x{self.workerId} Trade requested at {tradeDatetime}  Pool start at {poolStartDatetime}'
                f'x{self.workerId} For {name}, a trade farther in the past than the set of trades in the pool was requested by milliseconds.  '
                f'x{self.workerId} Calculation must always traverse trades from the past to the future. '
                f'x{self.workerId} function debug: {debug}'
            )
            os._exit(0)
        poolEndMilliseconds = self.getTradeMilliseconds(self.getLastInPool())
        if poolEndMilliseconds < targetMilliseconds:
            tradeDatetime = self.logTime(targetMilliseconds)
            poolEndDatetime = self.logTime(poolEndMilliseconds)
            logging.error(
                f'x{self.workerId} Trade requested at {tradeDatetime}  Pool end at {poolEndDatetime}\n'
                f'x{self.workerId} For {name}, a trade farther in the future than the set of trades\n'
                f'x{self.workerId} in the pool was requested by milliseconds.\n'
                f'x{self.workerId} function debug: {debug}'
            )
            os._exit(0)

    # @profile
    def getMiniPool(self, pivotIndex, miniPool, workerId):
        # processStepStart = timing.startCalculation()
        # logging.info('getMiniPoolStart')
        # timing.progressCalculation(processStepStart)
        miniPool.pivotIndex = pivotIndex
        miniPool.workerId = workerId
        miniPool.isMiniPool = True
        # miniPool.features = self.features
        pivotTrade = self.getTradeAt(pivotIndex)
        pivotTimeMilliseconds = self.getTradeMilliseconds(pivotTrade)
        endTimeMilliseconds = pivotTimeMilliseconds

        logging.debug(f'x{self.workerId} index: {pivotIndex} id: {pivotTrade[4]}')

        for timeName, periodMilliseconds in TradePool.features.TIME_PERIODS.items():
            startTimeMilliseconds = pivotTimeMilliseconds - periodMilliseconds
            self.setPastIndexes(f'{miniPool.workerId}_past_{timeName}', startTimeMilliseconds, pivotIndex)

        for timeName, periodMilliseconds in TradePool.features.FUTURE_TIME_PERIODS.items():
            endTimeMilliseconds = pivotTimeMilliseconds + periodMilliseconds
            self.setFurureIndexes(f'{miniPool.workerId}_future_{timeName}', endTimeMilliseconds, pivotIndex)

        logging.debug(f'x{self.workerId} subPools Items:')
        logging.debug(self.subPools.items())

        for name, indexes in self.subPools.items():
            miniPoolName = name.replace(f'{miniPool.workerId}_','')
            miniPool.subPools[miniPoolName] = {}
            miniPool.subPools[miniPoolName]['startIndex'] = self.subPools[name]['startIndex']
            miniPool.subPools[miniPoolName]['endIndex'] = pivotIndex

        miniPool.logPoolDetails()

        return miniPool

    def getLastNumberOfTrades(self):
        if not self.isMiniPool:
            logging.error( \
                f'x{self.workerId} getLastNumberOfTrades may only be called on a miniPool' \
            )

        collectedFeatures = {}
        pivotTrade = self.getTradeAt(self.pivotIndex)
        tradeFeatures = TradePool.features.FEATURE_INDEXES['exchange']

        for negativeIndex in range(1,TradePool.features.PREVIOUS_TRADE_COUNT):
            index = self.pivotIndex - negativeIndex
            negativeTrade = self.getTradeAt(index)
            # self.logTrade('negativeTrade', index, negativeTrade)
            # self.logTrade('   pivotTrade', self.pivotIndex, pivotTrade)
            priceKey = f'trade-{negativeIndex}-price'
            collectedFeatures[priceKey] = negativeTrade[0] - pivotTrade[0]
            volumeKey = f'trade-{negativeIndex}-volume'
            collectedFeatures[volumeKey] = 0 - negativeTrade[1]
            if negativeTrade[2] > 0:
                collectedFeatures[volumeKey] = negativeTrade[1]
            dateKey = f'trade-{negativeIndex}-date_ms'
            collectedFeatures[dateKey] = negativeTrade[3] - pivotTrade[3]

        return collectedFeatures

    def logTrade(self, name, index, trade):
        logging.info(f'x{self.workerId} name: {name} index: {index} price: {trade[0]} volume: {trade[0]} type: {trade[2]} date_ms: {trade[3]}')



    def getInbetweenMiniPools(self, pivotIndex, miniPool, workerId):
        miniPoolList = []

        miniPool.workerId = workerId
        miniPool.isMiniPool = True
        # miniPool.features = self.features
        pivotTrade = self.getTradeAt(pivotIndex)
        nextPivotTrade = self.getTradeAt(pivotIndex + 1)
        pivotTimeMilliseconds = self.getTradeMilliseconds(pivotTrade)
        nextPivotTimeMilliseconds = self.getTradeMilliseconds(nextPivotTrade)
        i = 1
        while nextPivotTimeMilliseconds > pivotTimeMilliseconds + ( 5000 * i ):
            pivotTimeMilliseconds = nextPivotTimeMilliseconds
            endTimeMilliseconds = pivotTimeMilliseconds

            logging.debug(f'x{self.workerId} GAP MINI POOL index: {pivotIndex} id: {pivotTrade[4]} second+: {i}')

            for timeName, periodMilliseconds in TradePool.features.TIME_PERIODS.items():
                startTimeMilliseconds = pivotTimeMilliseconds - periodMilliseconds
                self.setPastIndexes(f'{miniPool.workerId}_past_{timeName}', startTimeMilliseconds, pivotIndex)

            for timeName, periodMilliseconds in TradePool.features.FUTURE_TIME_PERIODS.items():
                targetMilliseconds = pivotTimeMilliseconds + periodMilliseconds
                self.setFurureIndexes(f'{miniPool.workerId}_future_{timeName}', targetMilliseconds, pivotIndex)

            logging.debug(f'x{self.workerId} subPools Items:')
            logging.debug(self.subPools.items())

            for name, indexes in self.subPools.items():
                miniPoolName = name.replace(f'{miniPool.workerId}_','')
                miniPool.subPools[miniPoolName] = {}
                miniPool.subPools[miniPoolName]['startIndex'] = self.subPools[name]['startIndex']
                miniPool.subPools[miniPoolName]['endIndex'] = pivotIndex


            miniPool.logPoolDetails()

            miniPoolList.append(miniPool)
            i = i + 1

        return miniPoolList

    def getFutureTrades(self, name):
        return self.futureTrades[name]

    def setFurureIndexes(self, name, endTimeMilliseconds, pivotIndex):
        if self.isMiniPool:
            logging.error(
                'Trade selection attempt in miniPool\n'
                'All trade selection must be done in parent pool.'
            )
            os._exit(0)
        if name not in self.subPools:
            self.addPool(name, True)

        logging.debug(f'x{self.workerId} For: {name} Start startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')

        self.subPools[name]['startIndex'] = pivotIndex

        logging.debug(f'x{self.workerId} For: {name} Begin checks startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')

        pivotTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(pivotIndex)))
        initalEndTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]['endIndex'])))
        targetEndTime = self.logTime(endTimeMilliseconds)
        diff = endTimeMilliseconds - self.getTradeMilliseconds(self.getTradeAt(pivotIndex))
        logging.debug(f'x{self.workerId} For: {name} PivotTimeDiff: {diff}')
        logging.debug(f'x{self.workerId} For: {name} PivotTime: {pivotTime} Inital startTime: {initalEndTime} Target startTime: {targetEndTime}')


        if self.getTradeMilliseconds(self.getLastInPool(name)) > endTimeMilliseconds:
            logging.debug(f'x{self.workerId} For: {name} Error startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')
            logging.warning(f'x{self.workerId} For: {name} A FUTURE trade selection happened out of order!!')
            os._exit(0)

        while self.getTradeMilliseconds(self.getFirstAfterPool(name)) < endTimeMilliseconds:
            self.subPools[name]['endIndex'] += 1

        if self.getTradeMilliseconds(self.getLastInPool(name)) > endTimeMilliseconds:
            self.subPools[name]['endIndex'] -= 1

        logging.debug(f'x{self.workerId} For: {name} Final startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')
        if self.subPools[name]["startIndex"] > self.subPools[name]["endIndex"]:
            logging.debug(f'x{self.workerId} For: {name} Error startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')
            logging.warning(f'x{self.workerId} For: {name} A FUTURE end traded ended up before start!!')
            os._exit(0)



    def setPastIndexes(self, name, startTimeMilliseconds, pivotIndex):
        if self.isMiniPool:
            logging.error(
                'Trade selection attempt in miniPool\n'
                'All trade selection must be done in parent pool.'
            )
            os._exit(0)
        if name not in self.subPools:
            self.addPool(name)

        self.subPools[name]['endIndex'] = pivotIndex

        initalStartTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]['startIndex'])))
        logging.debug(f'x{self.workerId} Inital startTime: {initalStartTime}')
        targetStartTime = self.logTime(startTimeMilliseconds)
        logging.debug(f'x{self.workerId} Target startTime: {targetStartTime}')

        if self.getTradeMilliseconds(self.getFirstBeforePool(name)) > startTimeMilliseconds:
            while self.getTradeMilliseconds(self.getFirstInPool(name)) > startTimeMilliseconds:
                self.subPools[name]['startIndex'] -= 1
                logging.warning(f'x{self.workerId} For: {name} A PAST trade selection happened out of order!!')
                os._exit(0)

        if self.getTradeMilliseconds(self.getSecondInPool(name)) < startTimeMilliseconds:
            while self.getTradeMilliseconds(self.getFirstInPool(name)) < startTimeMilliseconds:
                self.subPools[name]['startIndex'] += 1

        if self.subPools[name]["startIndex"] > self.subPools[name]["endIndex"]:
            logging.debug(f'x{self.workerId} Single index pool?')
            self.subPools[name]['startIndex'] -= 1

        logging.debug(f'x{self.workerId} Final startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')
        if self.subPools[name]["startIndex"] > self.subPools[name]["endIndex"]:
            logging.debug(f'x{self.workerId} For: {name} Error startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')
            logging.warning(f'x{self.workerId} For: {name} A PAST end traded ended up before start!!')
            os._exit(0)



