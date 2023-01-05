import mydb
import tradePool
import dataCalculate
import logger

selectTrades = """
SELECT price, amount, type, date_ms, trade_id
FROM trades
WHERE trade_id > ?
LIMIT %s
OFFSET ?
ORDER BY date_ms ASC
"""

selectFarthestCompleteTrade = """
SELECT trade_id
FROM tradesCalculated
LIMIT 1
ORDER BY trade_id DESC
"""

dbBatchIndex = 0
tradeBatchSize = 100000
farthestCompleteTradeId = mydb.selectOneStatic(selectFarthestCompleteTrade)
offsetId = farthestCompleteTradeId - tradeBatchSize

def main():
    offset = dbBatchIndex * tradeBatchSize
    dbBatchIndex += 2
    tradeList = mydb.selectAll(selectTrades % str(tradeBatchSize * 2), (offsetId, offset))
    tradePool = TradePool(tradeList)

    df = dataCalculate.setupDataFrame()
    while index < len(tradePool.getTradeList()):
        trade = tradePool.getTradeList()[index]
        if trade[4] > farthestCompleteTradeId:
            index = tryFeatureCalculation(df, tradePool, index, trade[4], trade[3])

    engine = mydb.getEngine()
    df.to_sql('tradesCalculated', con = engine, if_exists = 'append', chunksize = 1000)

def tryFeatureCalculation(df, tradePool, pivotTradeId, index, tradeTimeMilliSeconds):
    try:
        previousFeatures = dataCalculate.calculateAllFeatureGroups(df, tradePool, pivotTradeId, tradeTimeMilliSeconds)
        previousMilliseconds = trade[3]
        index += 1
        return index
    except AssertionError as error:
        logger.error(error)
        raise
    except IndexError as error:
        logger.error(error)
        if error.args[1]:
            if addedTradeCount := addMoreTrades(tradePool):
                return index - addedTradeCount
            raise StopIteration('No additional trades available to continue.')
        raise

def addMoreTrades(tradePool):
    offset = dbBatchIndex * tradeBatchSize
    dbBatchIndex += 1
    tradeList = mydb.selectAll(selectTrades % str(tradeBatchSize), (offsetId, offset))
    tradePool.rotateTradesIntoTheFuture(tradeList)
    return len(tradeList)

if __name__ == '__main__':
    try:
        main()
    except StopIteration:
        logger.error(error)
        print("script end reached")







