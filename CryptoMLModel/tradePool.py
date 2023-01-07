import logging
import datetime
# trades[0] price
# trades[1] amount
# trades[2] type
# trades[3] date_ms
# trades[4] trade_id

class TradePool:

    MILLISECONDS_GAP_TOLERATED = 60000

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
        return self.tradeList[startIndex:endIndex]

    def addPool(self, name):
        if name not in self.subPools:
            logging.debug(f'Adding index tracking for subPool: {name}')
            self.subPools[name] = {
                'startIndex': 0,
                'endIndex': -1
            }

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
            for name, indexes in self.subPools.items():
                indexes["startIndex"] -= n
                indexes["endIndex"] -= n
            self.maxIndex = len(self.tradeList)
            return True
        raise AssertionError(
            'An empty set of trades was recieved when trying to add more trades to a pool.'
        )

    def startIndexExistsCheck(self, listIndex):
        if listIndex == -1:
            raise IndexError(
                'A trade father in the past than the set of trades in the pool was requested by index.  '
                'Calculation must always traverse trades from the past to the future.',
                False
            )
        if listIndex > self.maxIndex:
            raise IndexError(
                'A trade father in the future than the set of trades in the pool was requested by index.',
                True
            )

    def endIndexExistsCheck(self, listIndex):
        if listIndex == 0:
            raise IndexError(
                'A trade father in the future than the set of trades in the pool was requested by index.',
                True
            )
        if listIndex < self.maxIndex * -1:
            raise IndexError(
                'A trade father in the past than the set of trades in the pool was requested by index.  '
                'Calculation must always traverse trades from the past to the future.',
                False
            )

    def isMillisecondsInPool(self, targetMilliseconds):
        poolStartMilliseconds = self.getTradeMilliseconds(self.getFirstInPool())
        if poolStartMilliseconds > targetMilliseconds:
            tradeDatetime = self.logTime(targetMilliseconds)
            poolStartDatetime = self.logTime(poolStartMilliseconds)
            raise IndexError(
                f'Trade requested at {tradeDatetime}  Pool start at {poolStartDatetime}'
                'A trade father in the past than the set of trades in the pool was requested by milliseconds.  '
                'Calculation must always traverse trades from the past to the future.',
                False
            )
        poolEndMilliseconds = self.getTradeMilliseconds(self.getLastInPool())
        if poolEndMilliseconds < targetMilliseconds:
            tradeDatetime = self.logTime(targetMilliseconds)
            poolEndDatetime = self.logTime(poolEndMilliseconds)
            raise IndexError(
                f'Trade requested at {tradeDatetime}  Pool end at {poolEndDatetime}'
                'A trade father in the future than the set of trades in the pool was requested by milliseconds.',
                True
            )

    def getTrades(self, name, timeGroup, pivotTradeId, startTimeMilliseconds, endTimeMilliseconds):
        if name not in self.subPools:
            self.addPool(name)
        logging.debug(f'Inital startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')
        logging.debug(f'Moving Indes for subPool: {name}')
        if timeGroup == 'future':
            return self.selectFutureTrade(name, endTimeMilliseconds)

        return self.selectPastTrades(name, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds)

    def selectFutureTrade(self, name, targetMilliseconds):
        self.isMillisecondsInPool(targetMilliseconds)
        self.startIndexExistsCheck(self.subPools[name]["startIndex"])
        self.subPools[name]["endIndex"] = self.subPools[name]["startIndex"] + 1

        initalStartTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]["startIndex"])))
        targetStartTime = self.logTime(targetMilliseconds)
        logging.debug(f'Inital startTime: {initalStartTime} Target startTime: {targetStartTime}')

        while self.getTradeMilliseconds(self.getFirstInPool(name)) > targetMilliseconds:
            self.subPools[name]["startIndex"] -= 1
            self.subPools[name]["endIndex"] -= 1
            self.startIndexExistsCheck(self.subPools[name]["startIndex"])

        while self.getTradeMilliseconds(self.getFirstInPool(name)) < targetMilliseconds:
            self.subPools[name]["startIndex"] += 1
            self.subPools[name]["endIndex"] += 1
            self.startIndexExistsCheck(self.subPools[name]["startIndex"])

        logging.debug(f'Final startIndex: {self.subPools[name]["startIndex"]}')

        return self.getTradeList(name)

    def selectPastTrades(self, name, startTimeMilliseconds, pivotTradeId, endTimeMilliseconds):
        self.isMillisecondsInPool(startTimeMilliseconds)
        self.isMillisecondsInPool(endTimeMilliseconds)
        self.startIndexExistsCheck(self.subPools[name]["startIndex"])
        self.endIndexExistsCheck(self.subPools[name]["endIndex"])

        initalStartTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]["startIndex"])))
        initalEndTime = self.logTime(self.getTradeMilliseconds(self.getTradeAt(self.subPools[name]["endIndex"])))
        logging.debug(f'Inital startTime: {initalStartTime} endTime: {initalEndTime}')
        targetStartTime = self.logTime(startTimeMilliseconds)
        targetEndTime = self.logTime(endTimeMilliseconds)
        logging.debug(f'Target startTime: {targetStartTime} endTime: {targetEndTime}')

        while self.getTradeMilliseconds(self.getFirstInPool(name)) > startTimeMilliseconds:
            self.subPools[name]["startIndex"] -= 1
            self.startIndexExistsCheck(self.subPools[name]["startIndex"])

        while self.getTradeMilliseconds(self.getFirstInPool(name)) < startTimeMilliseconds:
            self.subPools[name]["startIndex"] += 1
            self.startIndexExistsCheck(self.subPools[name]["startIndex"])


        while self.getTradeMilliseconds(self.getLastInPool(name)) > endTimeMilliseconds:
            self.subPools[name]["endIndex"] -= 1
            self.endIndexExistsCheck(self.subPools[name]["endIndex"])

        while self.getTradeMilliseconds(self.getLastInPool(name)) < endTimeMilliseconds:
            self.subPools[name]["endIndex"] += 1
            self.endIndexExistsCheck(self.subPools[name]["endIndex"])
            if pivotTradeId == self.getTradeId(self.getLastInPool(name)):
                break

        while pivotTradeId != self.getTradeId(self.getLastInPool(name)):
            self.subPools[name]["endIndex"] += 1
            self.endIndexExistsCheck(self.subPools[name]["endIndex"])

        logging.debug(f'Final startIndex: {self.subPools[name]["startIndex"]} endIndex: {self.subPools[name]["endIndex"]}')

        return self.getTradeList(name)
