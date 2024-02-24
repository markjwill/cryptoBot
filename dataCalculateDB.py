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
import shutil

import dataCalculate
import features as f
import timing
# import tradeDbManager as tdm
import bucketConnector as bc
import tradePool as tp

tradePool = False
features = False

def main(s3bucket, sourceBucketFileName, outputFolder):
    global tradePool, features, isTest
    isTest=""
    if "test" in sourceBucketFileName:
        isTest="-test"
    features = setupFeatures()
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

    cpuPercent = multiprocessing.cpu_count() / 100
    makeMiniPoolProcessCount = max(round(3 * cpuPercent),1)
    featureCalculationProcessCount = max(round(97 * cpuPercent),1)

    logging.info(f'          miniPool cpus: {makeMiniPoolProcessCount}')
    logging.info(f'featureCalculation cpus: {featureCalculationProcessCount}')

    isLogger = True
    featureCalculationProcessors = []
    for i in range(featureCalculationProcessCount):
        featureCalculationProcessor = Process(target=featureCalculationWorker, args=(
                featureCalculationQueue,
                featureCalculationProcessCount,
                isLogger,
                makeMiniPoolQueue,
                recordsTotal,
                outputFolder
                ))
        isLogger = False
        featureCalculationProcessors.append(featureCalculationProcessor)

    fileSavePids = []
    for featureCalculationProcessor in featureCalculationProcessors:
        featureCalculationProcessor.start()
        fileSavePids.append(featureCalculationProcessor.pid)

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
        logging.info('>>>>>>>>>>>> Parent Process Debug Start <<<<<<<<<<')
        logging.info(debugResourceUsage())
        # logging.info(debugChildProcess())
        # logging.info(active_children())
        logging.info('>>>>>>>>>>>> Parent Process Debug End <<<<<<<<<<<<')
        time.sleep(60)

    closeAndWaitForProcessors(makeMiniPoolProcessors, makeMiniPoolQueue)

    logging.info('Mini pool queue emptied, Feature calculation queue full')
    closeAndWaitForProcessors(featureCalculationProcessors, featureCalculationQueue)

    timing.endCalculation(batchCalculationStart, recordsTotal, recordsTotal)

    mergeCsvs(fileSavePids, features, s3bucket, outputFolder)

    logging.info(f'          miniPool cpus: {makeMiniPoolProcessCount}')
    logging.info(f'featureCalculation cpus: {featureCalculationProcessCount}')


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
        makeMiniPoolQueue.task_done()
    makeMiniPoolQueue.task_done()

def featureCalculationWorker(
            featureCalculationQueue,
            featureCalculationProcessCount,
            isLogger,
            makeMiniPoolQueue,
            recordsTotal,
            outputFolder
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

def mergeCsvs(fileSavePids, features, bucket, outputFolder):
    csvFile, csvWriter = openCsvFile(features, outputFolder)
    csvWriter.writerow(features.COLUMNS)
    csvFile.close()
    sourceFile = getOutputFilePath(outputFolder)
    appendFiles = []
    for pid in fileSavePids:
        appendFiles.append(getOutputFilePath(outputFolder, pid))

    with open(sourceFile,'wb') as wfd:
        for f in appendFiles:
            with open(f,'rb') as fd:
                shutil.copyfileobj(fd, wfd)
                wfd.write(b"\n")
            os.remove(f)

    # bc.uploadFile(sourceFile, bucket)

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
