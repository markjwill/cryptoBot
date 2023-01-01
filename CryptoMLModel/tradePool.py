# trades[0] price
# trades[1] amount
# trades[2] type
# trades[3] date_ms

class: t=TradePool

    MILLISECONDS_GAP_TOLERATED = 5000

    def __init__(self, ascendingOrderTrades):
          self.trades = ascendingOrderTrades
          self.childTradePools = {}
          self.noDataGaps()

    def getFirstInPool():
        return self.trades[0]

    def getLastInPool():
        return self.trades[-1]

    def getTradeId(trade):
        return trade[0]

    def getTradeAmount(trade):
        return trade[1]

    def getTradeType(trade):
        return trade[2]

    def getTradeMilliseconds(trade):
        return trade[3]

    def getTrades():
        return self.trades

    def addChildPool(name, trades):
        if name not in self.childTradePools:
            self.childTradePool[name] = TradePool(trades)

    def noDataGaps():
        previousTimeMilliSeconds = self.getTradeMilliseconds(self.getFirstInPool())
        for trade in trades:
            if self.getTradeMilliseconds(trade) - self.MILLISECONDS_GAP_TOLERATED > previousTimeMilliSeconds:
                return False
            previousTimeMilliSeconds = self.getTradeMilliseconds(trade)
        return True

    def getPoolStartIndex(previousTrades):
        startTradeId = self.getTradeId(previousTrades.getFirstInPool())
        for targetIndex, trade in previousTrades.getTrades():
            if self.getTradeId(trade) == startTradeId
                return targetIndex

    def getPoolEndIndex(previousTrades):
        endTradeId = self.getTradeId(previousTrades.getLastInPool())
        for targetIndex, trade in reversed(list(enumerate(previousTrades.getTrades()))):
            if self.getTradeId(trade) == endTradeId
                return targetIndex

    def selectTrades(previousTrades, startTimeMilliSeconds, endTimeMilliSeconds, previousPoolStartIndex, previousPoolEndIndex):
        if self.getTradeMilliseconds(self.getFirstInPool()) < startTimeMilliSeconds:
            return False
        if self.getTradeMilliseconds(self.getLastInPool()) > endTimeMilliSeconds:
            return False
        try:
            self.trades[previousPoolStartIndex]
            self.trades[previousPoolEndIndex]
        except IndexError:
            return False
        trades = previousTrades

        # check if we need to add trades to the start or remove trades from the start
        firstTradeMilliSeconds = self.getTradeMilliseconds(trades.getFirstInPool())

        while firstTradeMilliSeconds > startTimeMilliSeconds:
            previousPoolStartIndex -= 1
            trades.insert(0,self.trades[previousPoolStartIndex])
            firstTradeMilliSeconds = self.getTradeMilliseconds(trades.getFirstInPool())

        while firstTradeMilliSeconds < startTimeMilliSeconds:
            previousPoolStartIndex += 1
            trades.pop(0)
            firstTradeMilliSeconds = self.getTradeMilliseconds(trades.getFirstInPool())

        # check if we need to add trades to the end or remove trades from the end
        lastTradeMilliSeconds = self.getTradeMilliseconds(trades.getLastInPool())

        while lastTradeMilliSeconds > endTimeMilliSeconds:
            previousPoolEndIndex -= 1
            trades.pop()
            lastTradeMilliSeconds = self.getTradeMilliseconds(trades.getLastInPool())

        while lastTradeMilliSeconds < startTimeMilliSeconds:
            previousPoolEndIndex += 1
            trades.append(self.trades[previousPoolEndIndex])
            lastTradeMilliSeconds = self.getTradeMilliseconds(trades.getLastInPool())

        return trades, previousPoolStartIndex, previousPoolEndIndex