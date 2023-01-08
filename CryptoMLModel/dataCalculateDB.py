import mydb
import tradePool as tp
import dataCalculate
import logging
import tradeDbManager as tdm
import hashlib
import json

def main():
    calculateTableName = getUniqueTableName(
        dataCalculate.TIME_PERIODS, 
        dataCalculate.PERIOD_FEATURES
    )
    tradeManager = tdm.TradeDbManager()
    tradeManager.setCalculatedTableName(calculateTableName)
    farthestCompleteTradeId = tradeManager.getFarthestCompleteTradeId()
    tradeList = tradeManager.getStarterTradeList()
    tradePool = tp.TradePool()
    addMoreTrades(tradePool, tradeManager, 3)

    df = dataCalculate.setupDataFrame()
    index = 0
    testBatcher = 0
    while index < tradePool.maxIndex:
        trade = tradePool.getTradeAt(index)
        poolStartMilliseconds = tradePool.getTradeMilliseconds(tradePool.getFirstInPool(False))
        tradeDatetime = tradePool.logTime(trade[3])
        if farthestCompleteTradeId == 0:
            if poolStartMilliseconds + 1000 >= trade[3] - dataCalculate.MAX_PERIOD:
                index += 1
                logging.debug(f'Skipping trade recorded at {tradeDatetime}')
                continue
        if trade[4] > farthestCompleteTradeId:
            logging.debug(f'Attempting feature calcuation for tradeId {trade[4]} recorded at {tradeDatetime}')
            df, index = tryFeatureCalculation(df, tradePool, tradeManager, index, trade)
            logging.info(f'Completed feature calcuation for tradeId {trade[4]} recorded at {tradeDatetime}')
            farthestCompleteTradeId = trade[4]
            testBatcher += 1
            if testBatcher == 1000:
                break

    engine = mydb.getEngine()
    df.to_sql(calculateTableName, con = engine, if_exists = 'append', chunksize = 1000)

def tryFeatureCalculation(df, tradePool, tradeManager, index, pivotTrade):
    try:
        df = dataCalculate.calculateAllFeatureGroups(df, tradePool, pivotTrade)
        index += 1
        return df, index
    except AssertionError as error:
        logging.error(error)
        raise
    except IndexError as error:
        logging.error(error)
        if error.args[1]:
            if addedTradeCount := addMoreTrades(tradePool, tradeManager, 1):
                return df, max(0,index - addedTradeCount)
            raise StopIteration('No additional trades available to continue.')
        raise

def addMoreTrades(tradePool, tradeManager, batchMultiplier):
    logging.info('Adding more trades.')
    cumulativeCount = 0
    tradeList = tradeManager.getAdditionalTradeList(batchMultiplier)
    cumulativeCount += len(tradeList)
    tradePool.rotateTradesIntoTheFuture(tradeList)
    while tradePool.dataGaps():
        logging.info('Adding more trades to get past data gaps.')
        #consider a more fine grained approach here
        tradeList = tradeManager.getAdditionalTradeList(0.25)
        cumulativeCount += len(tradeList)
        tradePool.rotateTradesIntoTheFuture(tradeList)

    return cumulativeCount

    # Move to db manager
def getUniqueTableName(periods, features):
    combinedDictionary = periods | features
    dhash = hashlib.md5()
    encoded = json.dumps(combinedDictionary, sort_keys=True).encode()
    dhash.update(encoded)
    return f'tradesCalculated_{dhash.hexdigest()}'

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    try:
        main()
    except StopIteration:
        logging.error(error)
        print("script end reached")
