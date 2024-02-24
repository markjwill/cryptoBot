from collections import ChainMap
from datetime import date
from multiprocessing import JoinableQueue
from multiprocessing import Pool
from multiprocessing import Process
from multiprocessing import Queue
from multiprocessing import active_children
import argparse
import csv
import logging
import multiprocessing
import numpy as np
import os
import pandas as pd
import time
# import cProfile
import psutil

import dataCalculate
import features as f
import timing
# import tradeDbManager as tdm
import bucketConnector as bc
import tradePool as tp

tradePool = False
features = False

def main(s3bucket, sourceBucketFileName, outputFolder):
    isTest=""
    if "test" in sourceBucketFileName:
        isTest="-test"
    global tradePool, features
    features = setupFeatures()
    # tradeList = initTradeManager()
    filePath = f'{outputFolder}/{sourceBucketFileName}'
    tradeList = getDataFromBucket(filePath, s3bucket)
    setupTradePool(tradeList, features)

    del tradeList
    viableIndexes = tradePool.mapGapIterable()
    if not viableIndexes:
        logging.error('No viable indexes found.  Check data source and gaps.')
        os._exit(0)
    recordsTotal = len(viableIndexes)
    batchCalculationStart = timing.startCalculation()
    logging.info('Setup complete, beginning iteration')

    makeMiniPoolQueue = JoinableQueue()
    featureCalculationQueue = JoinableQueue()
    # fileSaveQueue = JoinableQueue()

    cpuPercent = multiprocessing.cpu_count() / 100
    makeMiniPoolProcessCount = max(round(3 * cpuPercent),1)
    featureCalculationProcessCount = max(round(97 * cpuPercent),1)
    # fileSaveProcessCount = max(round(6 * cpuPercent),2)

    logging.info(f'          miniPool cpus: {makeMiniPoolProcessCount}')
    logging.info(f'featureCalculation cpus: {featureCalculationProcessCount}')
    # logging.info(f'   fileSaveProcess cpus: {fileSaveProcessCount}')

    filePath = openCsvFileForWriting(features, outputFolder, isTest)

    fileSaveProcessors = []
    for i in range(fileSaveProcessCount):
        fileSaveProcessor = Process(target=fileSaveWorker, args=(fileSaveQueue, features, outputFolder, ))
        fileSaveProcessors.append(fileSaveProcessor)

    # fileSavePids = []
    # for fileSaveProcessor in fileSaveProcessors:
    #     fileSaveProcessor.start()
    #     fileSavePids.append(fileSaveProcessor.pid)

    isLogger = True
    featureCalculationProcessors = []
    for i in range(featureCalculationProcessCount):
        featureCalculationProcessor = Process(target=featureCalculationWorker, args=(
                featureCalculationQueue,
                featureCalculationProcessCount,
                isLogger,
                makeMiniPoolQueue,
                recordsTotal,
                filePath
                ))
        isLogger = False
        featureCalculationProcessors.append(featureCalculationProcessor)

    for featureCalculationProcessor in featureCalculationProcessors:
        featureCalculationProcessor.start()

    makeMiniPoolProcessors = []
    for i in range(makeMiniPoolProcessCount):
        makeMiniPoolProcessor = Process(target=makeMiniPoolWorker, args=(makeMiniPoolQueue,featureCalculationQueue, featureCalculationProcessCount, ))
        makeMiniPoolProcessors.append(makeMiniPoolProcessor)

    for makeMiniPoolProcessor in makeMiniPoolProcessors:
        makeMiniPoolProcessor.start()

    workerIndexGroups = np.array((np.array_split(viableIndexes, makeMiniPoolProcessCount)), dtype=object).tolist()
    del viableIndexes
    for i in range(makeMiniPoolProcessCount):
        x = [makeMiniPoolQueue.put(index) for index in workerIndexGroups[i]]
    del x

    logging.info('Mini pool queue full')
    while makeMiniPoolQueue.qsize() > 1000:
        # logging.info('>>>>>>>>>>>> Parent Process Debug Start <<<<<<<<<<')
        # logging.info('>>>>>>>>>>>> Parent Process Debug Start <<<<<<<<<<')
        logging.info('>>>>>>>>>>>> Parent Process Debug Start <<<<<<<<<<')
        # logging.info(f'globals: {globals().keys()} \nlocals: {locals().keys()}')
        logging.info(debugResourceUsage())
        # logging.info(debugChildProcess())
        # logging.info(active_children())
        logging.info('>>>>>>>>>>>> Parent Process Debug End <<<<<<<<<<<<')
        # logging.info('>>>>>>>>>>>> Parent Process Debug End <<<<<<<<<<<<')
        # logging.info('>>>>>>>>>>>> Parent Process Debug End <<<<<<<<<<<<')
        time.sleep(60)

    closeAndWaitForProcessors(makeMiniPoolProcessors, makeMiniPoolQueue)

    logging.info('Mini pool queue emptied, Feature calculation queue full')
    closeAndWaitForProcessors(featureCalculationProcessors, featureCalculationQueue)

    timing.endCalculation(batchCalculationStart, recordsTotal, recordsTotal)

    # logging.info('Feature calculation queue emptied, File save queue full')
    # closeAndWaitForProcessors(fileSaveProcessors, fileSaveQueue)

    # mergeAndSplitCsvs(fileSavePids, features, s3bucket, outputFolder, isTest)
    uploadFinishedCsv(filePath, s3bucket)

    # logging.info('File save queue emptied')
    logging.info(f'          miniPool cpus: {makeMiniPoolProcessCount}')
    logging.info(f'featureCalculation cpus: {featureCalculationProcessCount}')
    # logging.info(f'   fileSaveProcess cpus: {fileSaveProcessCount}')

def closeAndWaitForProcessors(processorList, queue):
    for processor in processorList:
        queue.put(None)

    for processor in processorList:
        processor.join()

    queue.join()

def makeMiniPoolWorker(makeMiniPoolQueue, featureCalculationQueue, featureCalculationProcessCount):
    global tradePool
    pid = multiprocessing.current_process().pid
    logging.info(f'Make mini pool worker started {pid}')
    maxFeatureCalculationQueueSize = featureCalculationProcessCount * 1000
    while True:
        index = makeMiniPoolQueue.get()
        if index is None:
            logging.info('None arrived in makeMiniPoolWorker')
            break
        logging.debug(f'mQ {str(makeMiniPoolQueue.qsize()).zfill(5)} fQ {str(featureCalculationQueue.qsize()).zfill(5)} sQ       process {pid} Making miniPool in queue for index {index}')
        miniPool = tradePool.getMiniPool(index, tp.TradePool('mini'), pid)
        while featureCalculationQueue.qsize() > maxFeatureCalculationQueueSize:
            time.sleep(1)
        featureCalculationQueue.put(miniPool)
        miniPoolList = tradePool.getInbetweenMiniPools(index, tp.TradePool('mini'), pid)
        logging.debug(f'mQ {str(makeMiniPoolQueue.qsize()).zfill(5)} fQ {str(featureCalculationQueue.qsize()).zfill(5)} sQ       process {pid} Made {len(miniPoolList)} gap miniPools after index {index}')
        # for miniPool in miniPoolList:
        #     while featureCalculationQueue.qsize() > maxFeatureCalculationQueueSize:
        #         time.sleep(1)
        #     featureCalculationQueue.put(miniPool)
        makeMiniPoolQueue.task_done()
    makeMiniPoolQueue.task_done()

def featureCalculationWorker(
            featureCalculationQueue,
            featureCalculationProcessCount,
            isLogger,
            makeMiniPoolQueue,
            recordsTotal,
            filePath
        ):
    global tradePool, features
    pid = multiprocessing.current_process().pid
    logging.info(f'Feature calculation worker started {pid}')
    csvFile, csvWriter = openCsvFile(features, outputFolder, pid)
    processStart = timing.startCalculation()
    logAfter = 1000
    processed = 0
    while True:
        miniPool = featureCalculationQueue.get()
        if miniPool is None:
            logging.info('None arrived in featureCalculationWorker')
            break
        logging.debug(f'mQ {str(makeMiniPoolQueue.qsize()).zfill(5)} fQ {str(featureCalculationQueue.qsize()).zfill(5)} process {pid} Calculating features in queue')
        row = dataCalculate.calculateAllFeaturesToList(miniPool, features)
        csvWriter.writerow(row)
        del miniPool, row

        processed += 1
        if processed % logAfter == 0 and isLogger:
            logging.info(f'mQ {str(makeMiniPoolQueue.qsize()).zfill(5)} fQ {str(featureCalculationQueue.qsize()).zfill(5)} process {pid} Calculating features in queue')
            logging.info(debugResourceUsage())
            combinedProcessesCompleted = processed * featureCalculationProcessCount
            timing.endCalculation(processStart, combinedProcessesCompleted, recordsTotal)
        featureCalculationQueue.task_done()

    csvFile.close()
    logging.info(f'Pid complete: {pid}')
    featureCalculationQueue.task_done()

def openCsvFile(features, outputFolder, identifier = ''):
    filePath = getOutputFilePath(outputFolder, identifier)
    truncateAndCreateFile = open(filePath, 'w+')
    truncateAndCreateFile.close()
    csvFile = open(filePath, 'a', buffering=65536)
    csvWriter = csv.writer(csvFile)
    # csvWriter.writerow(features.COLUMNS)
    # csvFile.close()
    return csvFile, csvWriter

def getOutputFilePath(outputFolder, pid = ''):
    global isTest
    if pid != '':
        pid = f'.{pid}'
    return f'{outputFolder}/{date.today()}-all-columns{isTest}.csv{pid}'

def appendCsv(sourceFile, targetFile):
    with open(sourceFile, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        data_to_append = list(reader)

    with open(targetFile, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data_to_append)

def mergeCsvs(fileSavePids, features, bucket, outputFolder):
    csvFile, csvWriter = openCsvFile(features, outputFolder)
    csvWriter.writerow(features.COLUMNS)
    csvFile.close()
    sourceFile = getOutputFilePath(outputFolder)
    for pid in fileSavePids:
        appendFile = getOutputFilePath(outputFolder, pid)
        appendCsv(sourceFile, appendFile)
        os.remove(appendFile)

    bc.uploadFile(sourceFile, bucket)


# def closeAndReopenFile(features, csvFile, csvWriter):
#     path = csvFile.name
#     csvFile.close()
#     del csvWriter
#     csvFile = open(path, 'a', buffering=65536)
#     fieldnames = [outputGroup for groupContents in features.csvFiles.values() for outputGroup in groupContents]
#     csvWriter = csv.DictWriter(csvFile, fieldnames=fieldnames)
#     return csvFile, csvWriter

# def fileSaveWorker(fileSaveQueue, features, outputFolder):
#     pid = multiprocessing.current_process().pid
#     logging.info(f'File save worker started {pid}')
#     csvFile, csvWriter = openCsvFileForWriting(features, pid, outputFolder)
#     count = 0
#     rows = []
#     # while True:
#     #     fileDestinations = fileSaveQueue.get()
#     #     if fileDestinations is None:
#     #         logging.info('None arrived in fileSaveWorker')
#     #         break
#     #     logging.debug(f'mQ       fQ       process {pid} Saving file in queue')
#     #     row = dict(ChainMap(*fileDestinations.values()))
#     #     del fileDestinations
#     #     csvWriter.writerow(row)
#     #     # if count % 10000:
#     #     #     csvFile, csvWriter = closeAndReopenFile(features, csvFile, csvWriter)
#     #     fileSaveQueue.task_done()
#     while True:
#         featureListRow = fileSaveQueue.get()
#         if featureListRow is None:
#             logging.info('None arrived in fileSaveWorker')
#             break
#         logging.debug(f'mQ       fQ       process {pid} Saving file in queue')
#         rows.append(featureListRow)
#         count += 1
#         if count % 1000:
#             csvWriter.writerows(rows)
#             rows = []
#         fileSaveQueue.task_done()
#     csvWriter.writerows(rows)
#     logging.info(f'Closing CSV file for process {pid}')
#     csvFile.close()
#     logging.info(f'Worker finished for process {pid}')
#     fileSaveQueue.task_done()

def debugChildProcess():
    current_process = psutil.Process(os.getpid())
    mem = current_process.memory_percent()
    output = f'parent PID {os.getpid()} mem: {mem}\n'
    for child in current_process.children(recursive=True):
        mem = child.memory_percent()
        output += f'child PID {child.pid} mem: {mem}\n'
    return output

def debugVariables():
    return f'\nglobals: {globals().keys()}'

def debugResourceUsage():
    # Getting all memory using os.popen()
    # total_memory, used_memory, free_memory = map(
    #     int, os.popen('free -t -m').readlines()[-1].split()[1:])
    load1, load5, load15 = psutil.getloadavg()
    cpu_usage = (load15/os.cpu_count()) * 100

    # return f'\nRAM memory % used: {round((used_memory/total_memory) * 100, 2)} \n' \
    return f'RAM memory % used: {psutil.virtual_memory()[2]} \n' \
        f'    RAM Used (GB): {round(psutil.virtual_memory()[3]/1000000000,2)} \n' \
        f'The CPU usage is : {round(cpu_usage,2)} \n' \
        f'The usage statistics of {os.getcwd()} is: \n' \
        f'{psutil.disk_usage(os.getcwd())}'



# def multiTreadSafeWriteToFile(rows, filePath, pid):
#     threadWriteName = f'{filePath}.{pid}'
#     while True:
#         if not os.path.isfile(filePath):
#             time.sleep(1)
#             continue
#         try:
#             os.rename(filePath, threadWriteName)
#         except OSError as e:
#             continue
#         csvFile = open(threadWriteName, 'a')
#         csvWriter = csv.writer(csvFile)
#         csvWriter.writerows(rows)
#         csvFile.close()
#         os.rename(threadWriteName, filePath)
#         break

#     return



def uploadFinishedCsv(filePath, bucket):
    bc.uploadFile(filePath, bucket)





def getDataFromBucket(fileName, bucket):
    df = bc.downloadFile(fileName, bucket)
    tupleData = tuple(df.itertuples(index=False, name=None))
    return tupleData

def initTradeManager():
    tradeDbManager = tdm.TradeDbManager()
    tradeList = tradeDbManager.getStarterTradeList()
    del tradeDbManager
    return tradeList

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

    parser.add_argument( '-bucket',
                         '--bucket',
                         default='crypto-bot-bucket',
                         help='Provide the name of the s3 bucket. Example -bucket crypto-bot-bucket, default=crypto-bot-bucket' )

    parser.add_argument( '-source',
                         '--source',
                         default='tradeData-test.csv',
                         help='Provide source data file name in s3. Example --source tradeData-test.csv, default=tradeData-test.csv' )

    parser.add_argument( '-folder',
                         '--folder',
                         help='Provide temp local folder. Example --source /home/admin/cryptoBot/csvFiles, required, there is no default' )


    args = parser.parse_args()

    logging.basicConfig( level=args.loglevel.upper() )
    logging.info( 'Logging now setup.' )
    timing.startTiming()

    try:
        # cProfile.runctx('main()',globals(),locals())
        main(args.bucket, args.source, args.folder)
    except StopIteration as error:
        logging.error(error)
    logging.info("script end reached")
