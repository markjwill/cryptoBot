import mydb
import tradePool as tp
import dataCalculate
import argparse
import logging
import tradeDbManager as tdm

def main():
    tradeManager = tdm.TradeDbManager()
    calculateTableName = tradeManager.getUniqueTableName(
        dataCalculate.TIME_PERIODS,
        dataCalculate.PERIOD_FEATURES | dataCalculate.NON_PERIOD_FEATURES
    )
    tradeManager.setMaxTimePeriod(dataCalculate.MAX_PERIOD)
    farthestCompleteTradeId = tradeManager.getFarthestCompleteTradeId()
    tradeList = tradeManager.getStarterTradeList()
    tradePool = tp.TradePool()
    addMoreTrades(tradePool, tradeManager, 1)

    df = dataCalculate.setupDataFrame()
    index = 0
    testBatcher = 0
    while index < tradePool.maxIndex:
        trade = tradePool.getTradeAt(index)
        poolStartMilliseconds = tradePool.getTradeMilliseconds(tradePool.getFirstInPool(False))
        tradeDatetime = tradePool.logTime(trade[3])
        if poolStartMilliseconds + 1000 >= trade[3] - dataCalculate.MAX_PERIOD:
            index += 1
            logging.debug(
                f'Skipping trade recorded at {tradeDatetime} for being '
                'less than MAX_PERIOD + 1 second into the tradesPool'
            )
            continue
        if trade[4] > farthestCompleteTradeId:
            logging.debug(f'Attempting feature calcuation for tradeId {trade[4]} recorded at {tradeDatetime}')
            df, index = tryFeatureCalculation(df, tradePool, tradeManager, index, trade)
            logging.info(f'Completed feature calcuation for tradeId {trade[4]} recorded at {tradeDatetime}')
            farthestCompleteTradeId = trade[4]
            # For inital testing purposes only
            testBatcher += 1
            if testBatcher == 100000:
                break
        logging.debug(f'Skipping trade recorded at {tradeDatetime} for being already calculated.')
        index += 1

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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument( '-log',
                         '--loglevel',
                         default='warning',
                         help='Provide logging level. Example --loglevel debug, default=warning' )

    args = parser.parse_args()

    logging.basicConfig( level=args.loglevel.upper() )
    logging.info( 'Logging now setup.' )
    try:
        main()
    except StopIteration:
        logging.error(error)
        print("script end reached")
