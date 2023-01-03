# trades[0] price
# trades[1] amount
# trades[2] type
# trades[3] date_ms
# trades[4] trade_id

class: TradePool

    MILLISECONDS_GAP_TOLERATED = 5000

    def __init__(self, ascendingOrderTradeList):
          self.tradeList = ascendingOrderTradeList
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
            return True
        return False

    def noDataGaps():
        previousTimeMilliSeconds = self.getTradeMilliseconds(self.getFirstInPool())
        for trade in trades:
            if self.getTradeMilliseconds(trade) - self.MILLISECONDS_GAP_TOLERATED > previousTimeMilliSeconds:
                return False
            previousTimeMilliSeconds = self.getTradeMilliseconds(trade)
        return True

    def getTrades(name, timeGroup, pivotTradeId, startTimeMilliSeconds, endTimeMilliSeconds):
        self.addChildPool(name, self.getTradeList())
        self.childTradePool[name].selectTrades(self, startTimeMilliSeconds, pivotTradeId, endTimeMilliSeconds)

        return self.childTradePool[name].getTradeList()

    def selectTrades(parentTrades, startTimeMilliSeconds, pivotTradeId, endTimeMilliSeconds):
        if self.getTradeMilliseconds(parentTrades.getFirstInPool()) < startTimeMilliSeconds:
            return False
        if self.getTradeMilliseconds(parentTrades.getLastInPool()) > endTimeMilliSeconds:
            return False
        try:
            parentTrades.tradeList[self.parentStartIndex]
            parentTrades.tradeList[self.parentEndIndex]
        except IndexError:
            return False

        # adjust here so that past timeGroups select last trades in pool via tradeId instead of milliesecond

        firstTradeMilliSeconds = self.getTradeMilliseconds(self.getFirstInPool())

        while firstTradeMilliSeconds > startTimeMilliSeconds:
            self.parentStartIndex -= 1
            trades.insert(0,parentTrades.trades[self.parentStartIndex])
            firstTradeMilliSeconds = self.getTradeMilliseconds(self.getFirstInPool())

        while firstTradeMilliSeconds < startTimeMilliSeconds:
            self.parentStartIndex += 1
            trades.pop(0)
            firstTradeMilliSeconds = self.getTradeMilliseconds(self.getFirstInPool())

        lastTradeMilliSeconds = self.getTradeMilliseconds(self.getLastInPool())

        while lastTradeMilliSeconds > endTimeMilliSeconds:
            self.parentEndIndex -= 1
            trades.pop()
            lastTradeMilliSeconds = self.getTradeMilliseconds(self.getLastInPool())

        while lastTradeMilliSeconds < startTimeMilliSeconds:
            self.parentEndIndex += 1
            trades.append(parentTrades.trades[self.parentEndIndex])
            lastTradeMilliSeconds = self.getTradeMilliseconds(self.getLastInPool())

        return trades