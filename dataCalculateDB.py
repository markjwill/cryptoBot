import mydb
import tradePool as tp
import dataCalculate
import argparse
import logging
import tradeDbManager as tdm
import timing
import features as f
import pandas as pd
import threading
import csv
import atexit
import time

csvFiles = {}
csvWriters = {}

def main():
    threadCount = 10
    features = setupFeatures()
    openCsvFilesForWriting(features, threadCount)
    tradeDbManager, tradeList = initTradeManager()
    tradePool = setupTradePool(tradeList)
    gapIndexMap = tradePool.mapGaps(features)
    gapMapIterator = iter(gapIndexMap)
    timing.log('Inital setup complete')


    nextGapStart = next(gapMapIterator)
    nextGapEnd = gapIndexMap[nextGapStart]
    index = 0
    count = 0
    logAfter = 500
    poolStartMilliseconds = tradePool.getTradeMilliseconds(tradePool.getFirstInPool())
    batchCalculationStart = timing.startCalculation()
    processStepStart = timing.startCalculation()
    while index < tradePool.maxIndex:
        if index >= nextGapStart:
            index += 1
            logging.debug(f'Skipping trade in gap with index {index}')
            if index > nextGapEnd:
                nextGapStart = next(gapMapIterator)
                nextGapEnd = gapIndexMap[nextGapStart]
            continue

        logging.info('getMiniPoolStart')
        timing.progressCalculation(processStepStart)
        processStepStart = timing.startCalculation()
        miniPool = tradePool.getMiniPool(index, features)

        pivotTrade = tradePool.getTradeAt(index)
        tradeDatetime = tradePool.logTime(pivotTrade[3])
        logging.info('tryFeatureCalculationStart')
        timing.progressCalculation(processStepStart)
        processStepStart = timing.startCalculation()
        logging.debug(f'Attempting feature calcuation for tradeId {pivotTrade[4]} recorded at {tradeDatetime}')
        tryFeatureCalculation(miniPool, features, count, threadCount)
        index += 1
        logging.debug(f'Completed feature calcuation for tradeId {pivotTrade[4]} recorded at {tradeDatetime}')
        # farthestCompleteTradeId = pivotTrade[4]

        count += 1
        if count % logAfter == 0:
            logging.info(f'Completed feature calcuation through {tradeDatetime}')
            logging.info(f'{(index - count)} trades skipped so far.')
            timing.endCalculation(batchCalculationStart, logAfter, tradeDbManager.APROXIMATE_RECORD_COUNT)
        if count == 100:
            break

def processDataSave(fileDestinations, thread):
    for filePart, data in fileDestinations.items():
        fileName = str(thread) + filePart
        thisThread = threading.Thread(target=appendToCsvFiles, args=(fileName, data, ))
        thisThread.setName(f'save {fileName}')
        thisThread.start()

def appendToCsvFiles(fileName, data):
    csvWriters[fileName].writerow(data)

def openCsvFilesForWriting(features, threadCount):
    outputFolder = '/csvFiles'

    for filePart, fieldnames in features.csvFiles.items():
        for thread in range(threadCount):
            fileName = str(thread) + filePart
            truncateAndCreateFile = open(f'{outputFolder}/{fileName}.csv', 'w+')
            truncateAndCreateFile.close()
            csvFiles[fileName] = open(f'{outputFolder}/{fileName}.csv', 'a')
            csvWriters[fileName] = csv.DictWriter(csvFiles[fileName], \
                fieldnames=fieldnames)
        csvWriters["0"+filePart].writeheader()

    atexit.register(closeCsvFiles)

def closeCsvFiles():
    for file in csvFiles.values():
        file.close()

def initTradeManager():
    tradeDbManager = tdm.TradeDbManager()
    tradeList = tradeDbManager.getStarterTradeList()
    return tradeDbManager, tradeList

def setupTradePool(tradeList):
    tradePool = tp.TradePool()
    tradePool.setInitalTrades(tradeList)
    return tradePool

def setupFeatures():
    features = f.Features()
    return features

def tryFeatureCalculation(tradePool, features, count, threadCount):
    try:
        # threadsAvailable = len(csvWriters)
        # # logging.info(threading.enumerate())
        # while threading.active_count() > threadsAvailable:
        #     logging.info(f"Active threads: {threading.active_count()} v {threadsAvailable}")
        #     time.sleep(0.0001)
        threadNumber = count % threadCount
        thisThread = threading.Thread(target=featureCalculationThread, args=(threadNumber, tradePool, features, ))
        thisThread.setName(f'calculating {tradePool.getTradeMilliseconds(tradePool.getLastInPool())}')
        thisThread.start()
    except AssertionError as error:
        logging.error(error)
        raise
    except IndexError as error:
        logging.error(error)
        os._exit(0)
        if error.args[1]:
            # if addedTradeCount := addMoreTrades(tradePool, tradeDbManager, 1):
            #     return max(0,index - addedTradeCount)
            raise StopIteration('No additional trades available to continue.')
        raise

def featureCalculationThread(thread, tradePool, features):
    fileDestinations = dataCalculate.calculateAllFeatureGroups(tradePool, features)
    processDataSave(fileDestinations, thread)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument( '-log',
                         '--loglevel',
                         default='warning',
                         help='Provide logging level. Example --loglevel debug, default=warning' )

    args = parser.parse_args()

    logging.basicConfig( level=args.loglevel.upper() )
    logging.info( 'Logging now setup.' )
    timing.startTiming()
    try:
        main()
    except StopIteration as error:
        logging.error(error)
    logging.info("script end reached")
