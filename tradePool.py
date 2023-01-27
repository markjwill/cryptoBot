import logging
import datetime
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

    def setInitalTrades(self, ascendingOrderTradeList):
        self.tradeList = ascendingOrderTradeList
        self.maxIndex = len(self.tradeList)

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
            raise IndexError(
                f'For {name}, a trade father in the past than the set of trades in the pool was requested by a start index at {listIndex}. '
                'Calculation must always traverse trades from the past to the future. '
                f'function debug: {debug}',
                False
            )
        if listIndex >= self.maxIndex:
            raise IndexError(
                f'For {name}, a trade father in the future than the set of trades in the pool was requested by a start index at {listIndex}. '
                f'function debug: {debug}',
                True
            )

    def endIndexExistsCheck(self, listIndex, name, debug):
        if listIndex >= 0:
            raise IndexError(
                f'For {name}, a trade father in the future than the set of trades in the pool was requested by an end index at {listIndex}. '
                f'function debug: {debug}',
                True
            )
        if listIndex < self.maxIndex * -1:
            raise IndexError(
                f'For {name}, a trade father in the past than the set of trades in the pool was requested by an end index at {listIndex}. '
                'Calculation must always traverse trades from the past to the future. '
                f'function debug: {debug}',
                False
            )

    def isMillisecondsInPool(self, targetMilliseconds, name, debug):
        poolStartMilliseconds = self.getTradeMilliseconds(self.getFirstInPool())
        if poolStartMilliseconds > targetMilliseconds:
            tradeDatetime = self.logTime(targetMilliseconds)
            poolStartDatetime = self.logTime(poolStartMilliseconds)
            raise IndexError(
                f'Trade requested at {tradeDatetime}  Pool start at {poolStartDatetime}'
                f'For {name}, a trade father in the past than the set of trades in the pool was requested by milliseconds.  '
                'Calculation must always traverse trades from the past to the future. '
                f'function debug: {debug}',
                False
            )
        poolEndMilliseconds = self.getTradeMilliseconds(self.getLastInPool())
        if poolEndMilliseconds < targetMilliseconds:
            tradeDatetime = self.logTime(targetMilliseconds)
            poolEndDatetime = self.logTime(poolEndMilliseconds)
            raise IndexError(
                f'Trade requested at {tradeDatetime}  Pool end at {poolEndDatetime} '
                f'For {name}, a trade father in the future than the set of trades in the pool was requested by milliseconds. '
                f'function debug: {debug}',
                True
            )

    def getTrades(self, name, timeGroup, pivotTradeId, startTimeMilliseconds, endTimeMilliseconds):
        if name not in self.subPools:
            self.addPool(name)
        logging.debug(f'Inital startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')
        logging.debug(f'Moving Indexs for subPool: {name}')
        if timeGroup == 'future':
            return self.selectFutureTrade(name, endTimeMilliseconds)
        pastTrades = self.selectPastTrades(name, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds)
        logging.debug(f'Getting {len(pastTrades)} trades for {name} {timeGroup} at tradeId {pivotTradeId}')
        return pastTrades

    def selectFutureTrade(self, name, targetMilliseconds):
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

    def selectPastTrades(self, name, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds):
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

        while pivotTradeId != self.getTradeId(self.getLastInPool(name)):
            self.subPools[name]["endIndex"] += 1
            self.endIndexExistsCheck(self.subPools[name]["endIndex"], name, 'subset last trade id != pivotTradeId')

        logging.debug(f'Final startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')

        return self.getTradeList(name)
