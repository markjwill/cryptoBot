import mydb
import tradePool
import dataCalculate
import logger

selectTrades = """
SELECT price, amount, type, date_ms, trade_id
FROM trades
LIMIT %s
OFFSET ?
ORDER BY date_ms ASC
"""

dbBatchIndex = 0
tradeBatchSize = 100000

def main():
    offset = dbBatchIndex * tradeBatchSize
    dbBatchIndex += 2
    tradeList = mydb.selectAll(selectTrades % str(tradeBatchSize * 2), (offset,))
    tradePool = TradePool(tradeList)

    df = dataCalculate.setupDataFrame()
    while index < len(tradePool.getTradeList()):
        trade = tradePool.getTradeList()[index]
        if trade[4] > farthestCompleteTradeId:
            index = tryFeatureCalculation(df, tradePool, index, trade[4], trade[3])

    df.saveToDb() #placeholder

def tryFeatureCalculation(df, tradePool, pivotTradeId, index, tradeTimeMilliSeconds):
    try:
        previousFeatures = dataCalculate.calculateAllFeatureGroups(df, tradePool, pivotTradeId, tradeTimeMilliSeconds)
        previousMilliseconds = trade[3]
        return index += 1
    except AssertionError as error:
        logger.error(error)
        raise
    except IndexError as error:
        logger.error(error)
        if error.args[1]:
            if addMoreTrades(tradePool)
                return index - tradeBatchSize
            raise StopIteration('Not enough trades available to continue.')
        raise

def addMoreTrades(tradePool)
    offset = dbBatchIndex * tradeBatchSize
    dbBatchIndex += 1
    tradeList = mydb.selectAll(selectTrades % str(tradeBatchSize), (offset,))
    tradePool.rotateTradesIntoTheFuture(tradeList)

if __name__ == '__main__':
    try:
        main()
    except StopIteration:
        logger.error(error)
        print("script end reached")







