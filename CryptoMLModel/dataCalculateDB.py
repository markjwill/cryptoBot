import mydb
import tradePool as tp
import dataCalculate
import logging
import tradeDbManager as tdm
import datetime

def main():
    tradeManager = tdm.TradeDbManager()
    farthestCompleteTradeId = tradeManager.getFarthestCompleteTradeId()
    tradeList = tradeManager.getStarterTradeList()
    try:
        tradePool = tp.TradePool(tradeList)
    except IndexError as error:
        logging.error(error)
        if error.args[1] and not addMoreTrades(tradePool, tradeManager):
            raise StopIteration('No additional trades available to continue.')

    poolStartMilliseconds = tradePool.getTradeMilliseconds(tradePool.getFirstInPool())
    df = dataCalculate.setupDataFrame()
    index = 0
    while index < len(tradePool.getTradeList()):
        trade = tradePool.getTradeList()[index]
        if farthestCompleteTradeId == 0:
            if poolStartMilliseconds > trade[3] - dataCalculate.MAX_PERIOD:
                logging.info(f'Skipping trade recorded at {tradeDatetime}')
                continue
        if trade[4] > farthestCompleteTradeId:
            tradeDatetime = datetime.datetime.fromtimestamp(tradelist[0][3]/1000.0).strftime('%Y-%m-%d %H:%M:%S')
            logging.info(f'Attempting feature calcuation for tradeId {trade[4]} recorded at {tradeDatetime}')
            index = tryFeatureCalculation(df, tradePool, tradeManager, index, trade)
            logging.info(f'Completed feature calcuation for tradeId {trade[4]} recorded at {tradeDatetime}')
            farthestCompleteTradeId = trade[4]
            break # For Testing Only

    engine = mydb.getEngine()
    df.to_sql('tradesCalculated', con = engine, if_exists = 'append', chunksize = 1000)

def tryFeatureCalculation(df, tradePool, tradeManager, index, pivotTrade):
    try:
        dataCalculate.calculateAllFeatureGroups(df, tradePool, pivotTrade)
        index += 1
        return index
    except AssertionError as error:
        logging.error(error)
        raise
    except IndexError as error:
        logging.error(error)
        if error.args[1]:
            if addedTradeCount := addMoreTrades(tradePool, tradeManager):
                return max(0,index - addedTradeCount)
            raise StopIteration('No additional trades available to continue.')
        raise

def addMoreTrades(tradePool, tradeManager):
    tradeList = tradeManager.getAdditionalTradeList()
    tradePool.rotateTradesIntoTheFuture(tradeList)
    return len(tradeList)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    try:
        main()
    except StopIteration:
        logging.error(error)
        print("script end reached")
