import mydb
import tradePool as tp
import dataCalculate
import argparse
import logging
import tradeDbManager as tdm
import timing
import features as f
import pandas as pd
import numpy as np
import threading
from datetime import date
import csv
import atexit
import time
import os
import multiprocessing
from multiprocessing import Pool
from multiprocessing import JoinableQueue
from multiprocessing import Queue
from multiprocessing import Process
from collections import ChainMap

# csvFiles = {}
# csvFile = None
# csvWriters = {}
# csvWriter = None
tradePool = False



def main():
    global tradePool
    features = setupFeatures()
    # openCsvFileForWriting(features)
    tradeDbManager, tradeList = initTradeManager()
    setupTradePool(tradeList, features)
    viableIndexes = tradePool.mapGapIterable()
    logAfter = 1000
    viableIndexes = viableIndexes[0:logAfter]
    index = 0
    batchCalculationStart = timing.startCalculation()
    logging.info('Setup complete, beginning iteration')

    makeMiniPoolQueue = JoinableQueue()
    featureCalculationQueue = JoinableQueue()
    fileSaveQueue = JoinableQueue()

    cpuTenPercent = multiprocessing.cpu_count() / 10
    makeMiniPoolProcessCount = max(round(3 * cpuTenPercent),1)
    featureCalculationProcessCount = max(round(5 * cpuTenPercent),1)
    fileSaveProcessCount = max(round(2 * cpuTenPercent),1)

    logging.info(f'          miniPool cpus: {makeMiniPoolProcessCount}')
    logging.info(f'featureCalculation cpus: {featureCalculationProcessCount}')
    logging.info(f'   fileSaveProcess cpus: {fileSaveProcessCount}')

    fileSaveProcessors = []
    for i in range(featureCalculationProcessCount):
        fileSaveProcessor = Process(target=fileSaveWorker, args=(fileSaveQueue, features, ))
        fileSaveProcessors.append(fileSaveProcessor)

    for fileSaveProcessor in fileSaveProcessors:
        fileSaveProcessor.start()

    # atexit.register(closeCsvFiles)

    featureCalculationProcessors = []
    for i in range(featureCalculationProcessCount):
        featureCalculationProcessor = Process(target=featureCalculationWorker, args=(featureCalculationQueue,fileSaveQueue,))
        featureCalculationProcessors.append(featureCalculationProcessor)

    for featureCalculationProcessor in featureCalculationProcessors:
        featureCalculationProcessor.start()

    makeMiniPoolProcessors = []
    for i in range(makeMiniPoolProcessCount):
        makeMiniPoolProcessor = Process(target=makeMiniPoolWorker, args=(makeMiniPoolQueue,featureCalculationQueue,))
        makeMiniPoolProcessors.append(makeMiniPoolProcessor)

    for makeMiniPoolProcessor in makeMiniPoolProcessors:
        makeMiniPoolProcessor.start()

    workerIndexGroups = np.array((np.array_split(viableIndexes, makeMiniPoolProcessCount))).tolist()
    for i in range(makeMiniPoolProcessCount):
        x = [makeMiniPoolQueue.put(index) for index in workerIndexGroups[i]]

    logging.info('Mini pool queue full')
    closeAndWaitForProcessors(makeMiniPoolProcessors, makeMiniPoolQueue)

    logging.info('Mini pool queue emptied, Feature calculation queue full')
    closeAndWaitForProcessors(featureCalculationProcessors, featureCalculationQueue)

    timing.endCalculation(batchCalculationStart, logAfter, tradeDbManager.APROXIMATE_RECORD_COUNT)

    logging.info('Feature calculation queue emptied, File save queue full')
    closeAndWaitForProcessors(fileSaveProcessors, fileSaveQueue)

    logging.info('File save queue emptied')

def closeAndWaitForProcessors(processorList, queue):
    for processor in processorList:
        queue.put(None)

    for processor in processorList:
        processor.join()

    queue.join()

def makeMiniPoolWorker(makeMiniPoolQueue, featureCalculationQueue):
    global tradePool
    pid = multiprocessing.current_process().pid
    logging.info(f'Make mini pool worker started {pid}')
    while True:
        index = makeMiniPoolQueue.get()
        if index is None:
            logging.info('None arrived in makeMiniPoolWorker')
            break
        logging.info(f'Making miniPool for index {index}')
        miniPool = tradePool.getMiniPool(index, tp.TradePool('mini'), pid)
        featureCalculationQueue.put(miniPool)
        makeMiniPoolQueue.task_done()
    makeMiniPoolQueue.task_done()

def featureCalculationWorker(featureCalculationQueue, fileSaveQueue):
    global tradePool
    pid = multiprocessing.current_process().pid
    logging.info(f'Feature calculation worker started {pid}')
    while True:
        logging.info(f'Waiting for miniPool in queue {pid}')
        miniPool = featureCalculationQueue.get()
        if miniPool is None:
            logging.info('None arrived in featureCalculationWorker')
            break
        fileDestinations = dataCalculate.calculateAllFeatureGroups(miniPool)
        fileSaveQueue.put(fileDestinations)
        featureCalculationQueue.task_done()
    featureCalculationQueue.task_done()

def fileSaveWorker(fileSaveQueue, features):
    pid = multiprocessing.current_process().pid
    logging.info(f'File save worker started {pid}')
    csvFile, csvWriter = openCsvFileForWriting(features, pid)
    while True:
        logging.info(f'Waiting for save data in queue {pid}')
        fileDestinations = fileSaveQueue.get()
        if fileDestinations is None:
            logging.info('None arrived in fileSaveWorker')
            break
        # logging.info(f'Save data arrived in queue, type: {type(fileDestinations)}')
        # logging.info(fileDestinations.values())
        row = dict(ChainMap(*fileDestinations.values()))
        # logging.info(row)
        csvWriter.writerow(row)
        fileSaveQueue.task_done()
    logging.info(f'Closing CSV file for process {pid}')
    csvFile.close()
    logging.info(f'Worker finished for process {pid}')
    fileSaveQueue.task_done()

def openCsvFileForWriting(features, pid):
    outputFolder = '/home/debby/bot/csvFiles'
    fileName = f'{date.today()}-all-columns-{pid}'
    truncateAndCreateFile = open(f'{outputFolder}/{fileName}.csv', 'w+')
    truncateAndCreateFile.close()
    csvFile = open(f'{outputFolder}/{fileName}.csv', 'a')
    fieldnames = [outputGroup for groupContents in features.csvFiles.values() for outputGroup in groupContents]
    # logging.info(f'CSVfieldnames {fieldnames}')
    csvWriter = csv.DictWriter(csvFile, fieldnames=fieldnames)
    csvWriter.writeheader()
    return csvFile, csvWriter



# def closeCsvFile():
#     global csvFile
#     logging.info('Closing CSV file')
#     csvFile.close()

# import csv

# with open('file.csv', 'r') as infile, open('reordered.csv', 'a') as outfile:
#     # output dict needs a list for new column ordering
#     fieldnames = ['A', 'C', 'D', 'E', 'B']
#     writer = csv.DictWriter(outfile, fieldnames=fieldnames)
#     # reorder the header first
#     writer.writeheader()
#     for row in csv.DictReader(infile):
#         # writes the reordered rows to the new file
#         writer.writerow(row)

# def fileSaveWorker():
#     logging.info('Process data save started')
#     while True:
#         logging.info('Waiting for save data in queue')
#         fileDestinations = fileSaveQueue.get()
#         if fileDestinations is None:
#             logging.info('None arrived in process data save queue')
#             break
#         logging.info(f'Save data arrived in queue, type: {type(fileDestinations)}')
#         threads = []
#         for fileName, data in fileDestinations.items():
#             newThread = threading.Thread(target=appendToCsvFiles, args=(fileName, data, ))
#             newThread.setName(f'save {fileName}')
#             threads.append(newThread)

#         for thread in threads:
#             thread.start()

#         for thread in threads:
#             thread.join()
#         fileSaveQueue.task_done()
#     logging.info('Data saves complete')
#     fileSaveQueue.task_done()




# def openCsvFilesForWriting(features):
#     outputFolder = '/home/debby/bot/csvFiles'

#     for fileName, fieldnames in features.csvFiles.items():
#         truncateAndCreateFile = open(f'{outputFolder}/{fileName}.csv', 'w+')
#         truncateAndCreateFile.close()
#         csvFiles[fileName] = open(f'{outputFolder}/{fileName}.csv', 'a')
#         csvWriters[fileName] = csv.DictWriter(csvFiles[fileName], \
#             fieldnames=fieldnames)
#         csvWriters[fileName].writeheader()

#     atexit.register(closeCsvFiles)

# def appendToCsvFiles(fileName, data):
#     # logging.info(data)
#     csvWriters[fileName].writerow(data)

# def closeCsvFiles():
#     for file in csvFiles.values():
#         file.close()


def initTradeManager():
    tradeDbManager = tdm.TradeDbManager()
    tradeList = tradeDbManager.getStarterTradeList()
    return tradeDbManager, tradeList

def setupTradePool(tradeList, features):
    tp.TradePool.tradeList = tradeList
    tp.TradePool.features = features
    global tradePool
    tradePool = tp.TradePool('parent')


def setupFeatures():
    features = f.Features()
    return features

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
