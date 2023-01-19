import mydb
import tradePool as tp
import dataCalculate
import argparse
import logging
import tradeDbManager as tdm
import timing
import features
import pandas as pd

def main(workerCount, workerIndex):
    batchSize = 50000
    features = setupFeatures()
    tradeDbManager, tradeList, farthestCompleteTradeId, calculatedTableName = setupTradeManager()
    tradePool = setupTradePool(tradeList)
    df = pd.DataFrame()
    timing.log('Inital setup complete')

    index = 0
    recordsInBatch = 0
    firstCalculation = True
    while index < tradePool.maxIndex:
        pivotTrade = tradePool.getTradeAt(index)
        poolStartMilliseconds = tradePool.getTradeMilliseconds(tradePool.getFirstInPool())
        tradeDatetime = tradePool.logTime(pivotTrade[3])
        if poolStartMilliseconds + 1000 >= pivotTrade[3] - features.MAX_PERIOD:
            index += 1
            logging.debug(
                f'Skipping trade recorded at {tradeDatetime} for being '
                'less than MAX_PERIOD + 1 second into the tradesPool'
            )
            continue

        if pivotTrade[4] <= farthestCompleteTradeId:
            index += 1
            logging.debug(f'Skipping trade recorded at {tradeDatetime} for being already calculated.')
            continue

        if firstCalculation == True:
            logging.info(f'Starting feature calcuation at {tradeDatetime}')
            timing.log('Starting first calcuation')
            batchCalculationStart = timing.startCalculation()
            firstCalculation = False

        logging.debug(f'Attempting feature calcuation for tradeId {pivotTrade[4]} recorded at {tradeDatetime}')
        df, index = tryFeatureCalculation(df, tradePool, tradeDbManager, features, index, pivotTrade)
        logging.debug(f'Completed feature calcuation for tradeId {pivotTrade[4]} recorded at {tradeDatetime}')
        farthestCompleteTradeId = pivotTrade[4]

        recordsInBatch += 1
        if recordsInBatch >= batchSize:
            timing.endCalculation(batchCalculationStart, recordsInBatch)
            timing.log(f'{recordsInBatch} calculations complete, Starting calcuated data save')
            logging.info(f'Completed feature calcuation through {tradeDatetime}')
            engine = mydb.getEngine()
            df.to_sql(calculatedTableName, con = engine, if_exists = 'append')
            engine.dispose()
            df = df[0:0]
            recordsInBatch = 0
            batchCalculationStart = timing.startCalculation()

def setupTradeManager(features):
    tradeDbManager = tdm.TradeDbManager()
    calculatedTableName = tradeDbManager.getUniqueTableName(
        features.getDictForTableName()
    )
    tradeDbManager.setMaxTimePeriod(features.MAX_PERIOD)
    farthestCompleteTradeId = tradeDbManager.getFarthestCompleteTradeId()
    tradeList = tradeDbManager.getStarterTradeList()
    return tradeDbManager, tradeList, farthestCompleteTradeId, calculatedTableName

def setupTradePool(tradeList):
    tradePool = tp.TradePool()
    tradePool.setInitalTrades(tradeList)
    return tradePool

def setupFeatures():
    features = Features()
    return features

def tryFeatureCalculation(df, tradePool, tradeDbManager, features, index, pivotTrade):
    try:
        df = dataCalculate.calculateAllFeatureGroups(df, tradePool, features, pivotTrade)
        index += 1
        return df, index
    except AssertionError as error:
        logging.error(error)
        raise
    except IndexError as error:
        logging.error(error)
        if error.args[1]:
            if addedTradeCount := addMoreTrades(tradePool, tradeDbManager, 1):
                return df, max(0,index - addedTradeCount)
            raise StopIteration('No additional trades available to continue.')
        raise

def addMoreTrades(tradePool, tradeDbManager, batchMultiplier):
    tradePool.logPoolDetails()
    logging.info('Starting adding more trades.')
    cumulativeCount = 0
    tradeList = tradeDbManager.getAdditionalTradeList(batchMultiplier)
    cumulativeCount += len(tradeList)
    tradePool.rotateTradesIntoTheFuture(tradeList)
    while tradePool.dataGaps():
        logging.info('Adding more trades to get past data gaps.')
        #consider a more fine grained approach here
        tradeList = tradeDbManager.getAdditionalTradeList(0.25)
        cumulativeCount += len(tradeList)
        tradePool.rotateTradesIntoTheFuture(tradeList)
    logging.info(f'Finished adding {cumulativeCount} more trades.')
    tradePool.logPoolDetails()
    return cumulativeCount

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument( '-log',
                         '--loglevel',
                         default='warning',
                         help='Provide logging level. Example --loglevel debug, default=warning' )

    parser.add_argument( '-i',
                         '--index',
                         default='warning',
                         help='Provide worker index. Example --index 2, default=warning' )

    parser.add_argument( '-workers',
                         '--workers',
                         default='warning',
                         help='Provide how many workers will be running. Example --workers 3, default=warning' )

    args = parser.parse_args()

    logging.basicConfig( level=args.loglevel.upper() )
    logging.info( 'Logging now setup.' )
    timing.startTiming()
    try:
        main()
    except StopIteration as error:
        logging.error(error)
    logging.info("script end reached")
