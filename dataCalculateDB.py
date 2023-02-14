import mydb
import tradePool as tp
import dataCalculate
import argparse
import logging
import tradeDbManager as tdm
import timing
import features as f
import pandas as pd
from threading import Thread
import csv
import atexit

csvFiles = {}
csvWriters = {}

def main(workerCount, workerIndex):
    batchSize = 5000
    features = setupFeatures()
    openCsvFilesForWriting(features)
    tradeDbManager, tradeList, farthestCompleteTradeId, calculatedTableName = setupTradeManager(
        features,
        workerCount,
        workerIndex
    )
    tradePool = setupTradePool(tradeList)
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
        index = tryFeatureCalculation(tradePool, tradeDbManager, features, index, pivotTrade)
        logging.debug(f'Completed feature calcuation for tradeId {pivotTrade[4]} recorded at {tradeDatetime}')
        farthestCompleteTradeId = pivotTrade[4]

        recordsPerWorker = tradeDbManager.APROXIMATE_RECORD_COUNT / workerCount

        recordsInBatch += 1
        if recordsInBatch % 100 == 0:
            logging.info(f'Completed feature calcuation through {tradeDatetime}')
            timing.endCalculation(batchCalculationStart, recordsInBatch, recordsPerWorker)

        if recordsInBatch >= batchSize:
            timing.log(f'{recordsInBatch} calculations complete, Starting calcuated data save')
            logging.info(f'Completed feature calcuation through {tradeDatetime}')
            timing.endCalculation(batchCalculationStart, recordsInBatch, recordsPerWorker)
            recordsInBatch = 0
            batchCalculationStart = timing.startCalculation()

def processDataSave(fileDestinations):
    for fileName, data in fileDestinations.items():
        Thread(target=appendToCsvFiles, args=(fileName, data, )).start()

def appendToCsvFiles(fileName, data):
    csvWriters[fileName].writerow(data)

def openCsvFilesForWriting(features):
    outputFolder = '/csvFiles'

    for fileName, fieldnames in features.csvFiles.items():
        truncateAndCreateFile = open(f'{outputFolder}/{fileName}.csv', 'w+')
        truncateAndCreateFile.close()
        csvFiles[fileName] = open(f'{outputFolder}/{fileName}.csv', 'a')
        csvWriters[fileName] = csv.DictWriter(csvFiles[fileName], \
            fieldnames=fieldnames)
        csvWriters[fileName].writeheader()

    atexit.register(closeCsvFiles)

def closeCsvFiles():
    for file in csvFiles.values():
        file.close()

def setupTradeManager(features, workerCount, workerIndex):
    tradeDbManager = tdm.TradeDbManager(workerCount, workerIndex)
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
    features = f.Features()
    return features

def tryFeatureCalculation(tradePool, tradeDbManager, features, index, pivotTrade):
    try:
        fileDestinations = dataCalculate.calculateAllFeatureGroups(tradePool, features, pivotTrade)
        processDataSave(fileDestinations)
        index += 1
        return index
    except AssertionError as error:
        logging.error(error)
        raise
    except IndexError as error:
        logging.error(error)
        if error.args[1]:
            if addedTradeCount := addMoreTrades(tradePool, tradeDbManager, 1):
                return max(0,index - addedTradeCount)
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

    # remove workers feature in favor of dask multi-threading
    parser.add_argument( '-w',
                         '--workerCount',
                         default=1,
                         const=1,
                         nargs='?',
                         help='Provide how many workers will be running. Example vaue: 3, default=1' )

    parser.add_argument( '-i',
                         '--workerIndex',
                         default=0,
                         const=0,
                         nargs='?',
                         help='Provide this workers index. Example value: 2, default=0' )

    parser.add_argument( '-log',
                         '--loglevel',
                         default='warning',
                         help='Provide logging level. Example --loglevel debug, default=warning' )

    args = parser.parse_args()

    logging.basicConfig( level=args.loglevel.upper() )
    logging.info( 'Logging now setup.' )
    timing.startTiming()
    try:
        main(int(args.workerCount), int(args.workerIndex))
    except StopIteration as error:
        logging.error(error)
    logging.info("script end reached")
