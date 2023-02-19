import logging
import datetime
import os
# trades[0] price
# trades[1] amount
# trades[2] type
# trades[3] date_ms
# trades[4] trade_id
# trades[5] coinbasePrice
# trades[6] huobiPrice
# trades[7] binancePrice

class TradePool:

    MILLISECONDS_GAP_TOLERATED = 90000

    tradeList = []
    maxIndex = 0
    subPools = {}
    pivotTradeId = 0
    isMiniPool = False
    futureTrades = {}

    def setInitalTrades(self, ascendingOrderTradeList, pivotTradeIndex=0, futureTrades={}):
        self.tradeList = ascendingOrderTradeList
        self.maxIndex = len(self.tradeList)
        self.pivotTradeIndex = pivotTradeIndex
        if self.pivotTradeIndex:
            self.isMiniPool = True
        self.futureTrades = futureTrades

    def getPivotTrade(self):
        if self.pivotTradeIndex == 0:
            raise AssertionError(
                'A pivot trade was requested, but none was set.'
            )
        return self.getTradeAt(self.pivotTradeIndex)

    def getFirstInPool(self, name=False):
        if not name:
            return self.tradeList[0]
        return self.tradeList[self.subPools[name]["startIndex"]]

    def getLastInPool(self, name=False):
        if not name:
            return self.tradeList[-1]
        return self.tradeList[self.subPools[name]["endIndex"]]

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
        startIndex = self.subPools[name]["startIndex"]
        endIndex = self.subPools[name]["endIndex"]
        if endIndex == -1:
            return self.tradeList[startIndex:]
        return self.tradeList[startIndex:endIndex + 1]

    def addPool(self, name):
        if name not in self.subPools:
            logging.debug(f'Adding index tracking for subPool: {name}')
            self.subPools[name] = {
                'startIndex': 0,
                'endIndex': -1
            }

    def logPoolDetails(self):
        if self.maxIndex == 0:
            logging.info('Trade pool is empty')
            return

        logging.info(f'Pool max index {self.maxIndex}')
        poolStartTime = self.logTime(self.getTradeMilliseconds(self.getFirstInPool()))
        poolEndTime = self.logTime(self.getTradeMilliseconds(self.getLastInPool()))
        logging.info(f'Pool startTime: {poolStartTime} endTime: {poolEndTime}')
        if logging.DEBUG == logging.getLogger().getEffectiveLevel():
            for name, indexes in self.subPools.items():
                logging.debug(f'{name} startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')
                subPoolStartTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]["startIndex"])))
                subPoolEndTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]["endIndex"])))
                logging.debug(f'{name} startTime: {subPoolStartTime} endTime: {subPoolEndTime}')
        logging.debug('logPoolDetails complete.')

    def miniPoolDataGaps(self, times):
        logging.debug('Starting data gap check')
        previousTimeMilliseconds = self.getTradeMilliseconds(self.getFirstInPool())
        for trade in self.tradeList[:-times]:
            # logging.debug(f"Gap compare {self.logTime(self.getTradeMilliseconds(trade) - self.MILLISECONDS_GAP_TOLERATED) } > {self.logTime(previousTimeMilliseconds)}")
            if ( self.getTradeMilliseconds(trade) - self.MILLISECONDS_GAP_TOLERATED ) > previousTimeMilliseconds:
                return True
            previousTimeMilliseconds = self.getTradeMilliseconds(trade)
        logging.debug('Finished data gap check')
        return False

    def dataGaps(self):
        logging.debug('Starting data gap check')
        previousTimeMilliseconds = self.getTradeMilliseconds(self.getFirstInPool())
        for trade in self.tradeList:
            # logging.debug(f"Gap compare {self.logTime(self.getTradeMilliseconds(trade) - self.MILLISECONDS_GAP_TOLERATED) } > {self.logTime(previousTimeMilliseconds)}")
            if ( self.getTradeMilliseconds(trade) - self.MILLISECONDS_GAP_TOLERATED ) > previousTimeMilliseconds:
                return True
            previousTimeMilliseconds = self.getTradeMilliseconds(trade)
        logging.debug('Finished data gap check')
        return False

    def rotateTradesIntoTheFuture(self, newTrades):
        if self.maxIndex == 0:
            return self.setInitalTrades(newTrades)
        if n := len(newTrades):
            del self.tradeList[:n]
            self.tradeList = self.tradeList + newTrades
            self.maxIndex = len(self.tradeList)
            for name, indexes in self.subPools.items():
                indexes["startIndex"] = 0
                indexes["endIndex"] = -1
            return
        raise AssertionError(
            'An empty set of trades was recieved when trying to add more trades to a pool.'
        )

    def startIndexExistsCheck(self, listIndex, name, debug):
        if listIndex <= -1:
            logging.error(
                f'For {name}, a trade father in the past than the set of trades in the pool was requested by a start index at {listIndex}. '
                'Calculation must always traverse trades from the past to the future. '
                f'function debug: {debug}'
            )
            os._exit(0)
        if listIndex >= self.maxIndex:
            logging.error(
                f'For {name}, a trade father in the future than the set of trades in the pool was requested by a start index at {listIndex}. '
                f'function debug: {debug}'
            )
            os._exit(0)

    def endIndexExistsCheck(self, listIndex, name, debug):
        if listIndex >= 0:
            logging.error(
                f'For {name}, a trade father in the future than the set of trades in the pool was requested by an end index at {listIndex}. '
                f'function debug: {debug}'
            )
            os._exit(0)
        if listIndex < self.maxIndex * -1:
            logging.error(
                f'For {name}, a trade father in the past than the set of trades in the pool was requested by an end index at {listIndex}. '
                'Calculation must always traverse trades from the past to the future. '
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
                f'For {name}, a trade father in the past than the set of trades in the pool was requested by milliseconds.  '
                'Calculation must always traverse trades from the past to the future. '
                f'function debug: {debug}'
            )
            os._exit(0)
        poolEndMilliseconds = self.getTradeMilliseconds(self.getLastInPool())
        if poolEndMilliseconds < targetMilliseconds:
            tradeDatetime = self.logTime(targetMilliseconds)
            poolEndDatetime = self.logTime(poolEndMilliseconds)
            logging.error(
                f'Trade requested at {tradeDatetime}  Pool end at {poolEndDatetime} '
                f'For {name}, a trade father in the future than the set of trades in the pool was requested by milliseconds. '
                f'function debug: {debug}'
            )
            os._exit(0)

    def getMiniPool(self, pivotTrade, features):
        name = 'miniPool'
        if name not in self.subPools:
            self.addPool(name)
        pivotTimeMilliseconds = self.getTradeMilliseconds(pivotTrade)
        startTimeMilliseconds = pivotTimeMilliseconds - features.MAX_PERIOD
        endTimeMilliseconds = pivotTimeMilliseconds

        tradeList = self.selectMultipleTrades(name, startTimeMilliseconds, self.getTradeId(pivotTrade), endTimeMilliseconds)
        listPivotIndex = len(tradeList) - 1

        futureTrades = {}
        for timeName, periodMilliseconds in features.TIME_PERIODS.items():
            targetMilliseconds =  pivotTimeMilliseconds + periodMilliseconds
            tradeItem = self.selectFutureTrade(timeName, targetMilliseconds)
            futureTrades[f'future_{timeName}'] = tradeItem

        logging.debug(f'Getting {len(tradeList)} trades for {name} at tradeId {self.getTradeId(pivotTrade)}')

        return tradeList, listPivotIndex, futureTrades

    def getTrades(self, name, timeGroup, pivotTradeId, startTimeMilliseconds, endTimeMilliseconds):
        if name not in self.subPools:
            self.addPool(name)
        logging.debug(f'Inital startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')
        logging.debug(f'Moving Indexs for subPool: {name}')
        if timeGroup == 'future':
            return self.selectFutureTrade(name, endTimeMilliseconds)
        pastTrades = self.selectMultipleTrades(name, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds)
        logging.debug(f'Getting {len(pastTrades)} trades for {name} {timeGroup} at tradeId {pivotTradeId}')
        return pastTrades

    def selectFutureTrade(self, name, targetMilliseconds):
        if name in self.futureTrades:
            return self.futureTrades[name]
        if name not in self.subPools:
            self.addPool(name)
        if not self.isMiniPool:
            self.isMillisecondsInPool(targetMilliseconds, name, 'target time > pool start time and < pool end time')
            self.startIndexExistsCheck(self.subPools[name]["startIndex"], name, 'inital index check')
        self.subPools[name]["endIndex"] = self.subPools[name]["startIndex"]

        initalStartTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]["startIndex"])))
        targetStartTime = self.logTime(targetMilliseconds)
        logging.debug(f'Inital startTime: {initalStartTime} Target startTime: {targetStartTime}')

        while self.getTradeMilliseconds(self.getFirstInPool(name)) < targetMilliseconds:
            self.subPools[name]["startIndex"] += 1
            self.subPools[name]["endIndex"] += 1
            self.startIndexExistsCheck(self.subPools[name]["startIndex"], name, 'subset target trade < target time')

        while self.getTradeMilliseconds(self.getFirstInPool(name)) > targetMilliseconds:
            self.subPools[name]["startIndex"] -= 1
            self.subPools[name]["endIndex"] -= 1
            self.startIndexExistsCheck(self.subPools[name]["startIndex"], name, 'subset target trade > target time')

        logging.debug(f'Final startIndex: {self.subPools[name]["startIndex"]}')

        return self.getTradeList(name)

    def selectMultipleTrades(self, name, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds):
        if name not in self.subPools:
            self.addPool(name)
        if not self.isMiniPool:
            self.isMillisecondsInPool(startTimeMilliseconds, name, 'start target time > pool start time and < pool end time')
            self.isMillisecondsInPool(endTimeMilliseconds, name, 'end taret time > pool start time and < pool end time')
        self.startIndexExistsCheck(self.subPools[name]["startIndex"], name, 'inital subpool start index check')
        self.endIndexExistsCheck(self.subPools[name]["endIndex"], name, 'inital subpool end index check')

        initalStartTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]["startIndex"])))
        initalEndTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]["endIndex"])))
        logging.debug(f'Inital startTime: {initalStartTime} endTime: {initalEndTime}')
        targetStartTime = self.logTime(startTimeMilliseconds)
        targetEndTime = self.logTime(endTimeMilliseconds)
        logging.debug(f'Target startTime: {targetStartTime} endTime: {targetEndTime}')

        if name != 'past_twoHours':
            while self.getTradeMilliseconds(self.getFirstInPool(name)) > startTimeMilliseconds:
                self.subPools[name]["startIndex"] -= 1
                self.startIndexExistsCheck(self.subPools[name]["startIndex"], name, 'subset first trade > start time')

        while self.getTradeMilliseconds(self.getFirstInPool(name)) < startTimeMilliseconds:
            self.subPools[name]["startIndex"] += 1
            self.startIndexExistsCheck(self.subPools[name]["startIndex"], name, 'subset first trade < start time')


        while self.getTradeMilliseconds(self.getLastInPool(name)) > endTimeMilliseconds:
            self.subPools[name]["endIndex"] -= 1
            self.endIndexExistsCheck(self.subPools[name]["endIndex"], name, 'subset last trade > end time')

        while self.getTradeMilliseconds(self.getLastInPool(name)) < endTimeMilliseconds:
            self.subPools[name]["endIndex"] += 1
            self.endIndexExistsCheck(self.subPools[name]["endIndex"], name, 'subset last trade < end time')
            if pivotTradeId == self.getTradeId(self.getLastInPool(name)):
                break

        logging.debug(f'pivotTradeId: {pivotTradeId} lastTradeId: {self.getTradeId(self.getLastInPool(name))} endIndex: {self.subPools[name]["endIndex"]}')
        while pivotTradeId != self.getTradeId(self.getLastInPool(name)):
            self.subPools[name]["endIndex"] += 1
            self.endIndexExistsCheck(self.subPools[name]["endIndex"], name, 'subset last trade id != pivotTradeId')

        logging.debug(f'Final startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')

        return self.getTradeList(name)
