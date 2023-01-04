# trades[0] price
# trades[1] amount
# trades[2] type
# trades[3] date_ms
# trades[4] trade_id

class: TradePool

    MILLISECONDS_GAP_TOLERATED = 5000

    def __init__(self, ascendingOrderTradeList):
          self.tradeList = ascendingOrderTradeList
          self.maxIndex = len(self.tradeList)
          self.childTradePools = {}
          self.parentStartIndex = 0
          self.parentEndIndex = -1

    def getFirstInPool():
        return self.tradeList[0]

    def getLastInPool():
        return self.tradeList[-1]

    def getTradePrice(trade):
        return trade[0]

    def getTradeAmount(trade):
        return trade[1]

    def getTradeType(trade):
        return trade[2]

    def getTradeMilliseconds(trade):
        return trade[3]

    def getTradeId(trade):
        return trade[4]

    def getTradeList():
        return self.tradeList

    def addChildPool(name, trades):
        if name not in self.childTradePools:
            self.childTradePool[name] = TradePool(trades)

    # Marking for removal, I think we will only ever allow
    # computing from past into the futuer
    # def rotateTradesIntoThePast(newTrades):
    #     if n := len(newTrades):
    #         del self.tradeList[-n:]
    #         self.tradeList = newTrades + self.tradeList
    #         return True
    #     return False

    def rotateTradesIntoTheFuture(newTrades):
        if n := len(newTrades):
            del self.tradeList[:n]
            self.tradeList = self.tradeList + newTrades
            for name, tradePool in self.childTradePools:
                tradePool.parentStartIndex = 0
                self.parentEndIndex = -1
            return True
        raise AssertionError(
            'An empty set of trades was recieved when trying to add more trades to a pool.'
        )

    def indexExistsCheck(tradePool, listIndex):
        if listIndex == -1:
            raise IndexError(
                'A trade father in the past than the set of trades in the pool was requested by index.\n'
                'Calculation must always traverse trades from the past to the future.',
                False
            )
        if listIndex > tradePool.maxIndex:
            raise IndexError(
                'A trade father in the future than the set of trades in the pool was requested by index.',
                True
            )

    def millisecondsRangeCheck(tradePool, startTimeMilliSeconds, endTimeMilliSeconds):
        if self.getTradeMilliseconds(tradePool.getFirstInPool()) < startTimeMilliSeconds:
            raise IndexError(
                'A trade father in the past than the set of trades in the pool was requested by milliseconds.\n'
                'Calculation must always traverse trades from the past to the future.',
                False
            )
        if self.getTradeMilliseconds(tradePool.getLastInPool()) > endTimeMilliSeconds:
            raise IndexError(
                'A trade father in the future than the set of trades in the pool was requested by milliseconds.',
                True
            )

    def noDataGaps():
        previousTimeMilliSeconds = self.getTradeMilliseconds(self.getFirstInPool())
        for trade in trades:
            if self.getTradeMilliseconds(trade) - self.MILLISECONDS_GAP_TOLERATED > previousTimeMilliSeconds:
                return False
            previousTimeMilliSeconds = self.getTradeMilliseconds(trade)
        return True

    def getTrades(name, timeGroup, pivotTradeId, startTimeMilliSeconds, endTimeMilliSeconds):
        if name not in self.childTradePools:
            self.addChildPool(name, self.getTradeList())
        self.childTradePool[name].selectTrades(self, startTimeMilliSeconds, pivotTradeId, endTimeMilliSeconds)

        return self.childTradePool[name].getTradeList()

    def selectPastTrades(parentTrades, startTimeMilliSeconds, pivotTradeId, endTimeMilliSeconds):
        self.millisecondsRangeCheck(parentTrades, startTimeMilliSeconds, endTimeMilliSeconds)
        self.indexExistsCheck(parentTrades, self.parentStartIndex)
        self.indexExistsCheck(parentTrades, self.parentEndIndex)

        # adjust here so that past timeGroups select last trades in pool via tradeId instead of milliesecond

        firstTradeMilliSeconds = self.getTradeMilliseconds(self.getFirstInPool())

        while firstTradeMilliSeconds > startTimeMilliSeconds:
            self.parentStartIndex -= 1
            self.indexExistsCheck(parentTrades, self.parentStartIndex)
            trades.insert(0,parentTrades.trades[self.parentStartIndex])
            firstTradeMilliSeconds = self.getTradeMilliseconds(self.getFirstInPool())

        while firstTradeMilliSeconds < startTimeMilliSeconds:
            self.parentStartIndex += 1
            self.indexExistsCheck(parentTrades, self.parentStartIndex)
            trades.pop(0)
            firstTradeMilliSeconds = self.getTradeMilliseconds(self.getFirstInPool())

        lastTradeMilliSeconds = self.getTradeMilliseconds(self.getLastInPool())

        while lastTradeMilliSeconds > endTimeMilliSeconds:
            self.parentEndIndex -= 1
            self.indexExistsCheck(parentTrades, self.parentEndIndex)
            trades.pop()
            lastTradeMilliSeconds = self.getTradeMilliseconds(self.getLastInPool())

        while lastTradeMilliSeconds < endTimeMilliSeconds:
            self.parentEndIndex += 1
            self.indexExistsCheck(parentTrades, self.parentEndIndex)
            trades.append(parentTrades.trades[self.parentEndIndex])
            lastTradeMilliSeconds = self.getTradeMilliseconds(self.getLastInPool())
            if pivotTradeId == self.getTradeId(self.getLastInPool()):
                continue

        while pivotTradeId != self.getTradeId(self.getLastInPool()):
            self.parentEndIndex += 1
            self.indexExistsCheck(parentTrades, self.parentEndIndex)
            trades.append(parentTrades.trades[self.parentEndIndex])

        return trades
