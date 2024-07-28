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
import os
import dataCalculate
import features as f
import timing
# import tradeDbManager as tdm
import bucketConnector as bc
import tradePool as tp
import datetime
import boto3
import logToCloudwatch as cw
import random

if os.getuid() != 0:
    logging.error("This program is not run as sudo or elevated this it will not work")
    os._exit(0)


def main(s3bucket, sourceBucketFileName, outputFolder):

    isTest=""
    if "test" in sourceBucketFileName:
        isTest="-test"
    features = setupFeatures()
    filePath = f'{outputFolder}/{sourceBucketFileName}'
    tradeList = getDataFromBucket(filePath, s3bucket)
    tradePool = setupTradePool(tradeList, features)

    del tradeList
    logging.info(f'Getting viable indexes')
    viableIndexes = tradePool.mapGapIterable()
    if not viableIndexes:
        logging.error('No viable indexes found.  Check data source and gaps.')
        os._exit(0)
    recordsTotal = len(viableIndexes)
    batchCalculationStart = timing.startCalculation()
    logging.info(f'Setup complete, beginning iteration on {recordsTotal} records')

    makeMiniPoolQueue = JoinableQueue()
    featureCalculationQueue = JoinableQueue()
    resultLoggerQueue = JoinableQueue()

    cpuPercent = multiprocessing.cpu_count() / 100
    makeMiniPoolProcessCount = max(round(3 * cpuPercent),1)
    featureCalculationProcessCount = max(round(75 * cpuPercent),1)

    logging.info(f'          miniPool cpus: {makeMiniPoolProcessCount}')
    logging.info(f'featureCalculation cpus: {featureCalculationProcessCount}')

    awsRegion = 'ca-central-1'
    logGroupName = 'ML-Log-Group'
    columnNames = ",".join(features.COLUMNS)
    cloudLogger = cw.CloudLogger(awsRegion, logGroupName, columnNames, logging)
    logging.info('Cloud Logged column names');
    maxQueueSize = 10000

    resultLoggerProcessor = Process(target=resultLoggerWorker, args=(
            resultLoggerQueue,
            cloudLogger,
        ))

    isLogger = True
    featureCalculationProcessors = []
    for i in range(featureCalculationProcessCount):
        featureCalculationProcessor = Process(target=featureCalculationWorker, args=(
                featureCalculationQueue,
                featureCalculationProcessCount,
                isLogger,
                makeMiniPoolQueue,
                resultLoggerQueue,
                recordsTotal,
                outputFolder,
                features,
                maxQueueSize,
            ))
        isLogger = False
        featureCalculationProcessors.append(featureCalculationProcessor)

    resultLoggerProcessor.start()

    # fileSavePids = []
    for featureCalculationProcessor in featureCalculationProcessors:
        featureCalculationProcessor.start()
        # fileSavePids.append(featureCalculationProcessor.pid)

    makeMiniPoolProcessors = []
    for i in range(makeMiniPoolProcessCount):
        makeMiniPoolProcessor = Process(target=makeMiniPoolWorker, 
            args=(
                makeMiniPoolQueue,
                featureCalculationQueue,
                tradePool,
                maxQueueSize,
            )
        )
        makeMiniPoolProcessors.append(makeMiniPoolProcessor)

    for makeMiniPoolProcessor in makeMiniPoolProcessors:
        makeMiniPoolProcessor.start()

    time.sleep(1)
    logging.info('Throttled sending of indexes to Mini pool queue')

    pointer = 0
    endPointer = 0
    while endPointer < len(viableIndexes):
        endPointer = pointer + maxQueueSize
        if endPointer > len(viableIndexes):
            endPointer = len(viableIndexes)
        listChunk = viableIndexes[pointer:endPointer]
        [makeMiniPoolQueue.put(index) for index in listChunk]
        while makeMiniPoolQueue.qsize() > maxQueueSize:
            # logging.info('MAX MINI QUEUE HIT')
            time.sleep(0.1)
        pointer = endPointer

    logging.info('Mini pool queue full')

    closeAndWaitForProcessors(makeMiniPoolProcessors, makeMiniPoolQueue)

    logging.info('Mini pool queue emptied, Feature calculation queue full')
    closeAndWaitForProcessors(featureCalculationProcessors, featureCalculationQueue)

    resultLoggerQueue.put(None)
    resultLoggerProcessor.join()
    resultLoggerQueue.join()

    timing.endCalculation(batchCalculationStart, recordsTotal, recordsTotal)

    # mergeCsvs(fileSavePids, features, s3bucket, outputFolder)

    logging.info(f'          miniPool cpus: {makeMiniPoolProcessCount}')
    logging.info(f'featureCalculation cpus: {featureCalculationProcessCount}')


def closeAndWaitForProcessors(processorList, queue):
    for processor in processorList:
        queue.put(None)

    for processor in processorList:
        processor.join()

    queue.join()

def makeMiniPoolWorker(
            makeMiniPoolQueue,
            featureCalculationQueue,
            tradePool,
            maxQueueSize
        ):

    pid = multiprocessing.current_process().pid
    logging.info(f'x{pid} Make mini pool worker started {pid}')
    while True:
        index = makeMiniPoolQueue.get()
        if index is None:
            logging.info(f'x{pid} None arrived in makeMiniPoolWorker')
            break
        logging.debug(f'x{pid} mQ {str(makeMiniPoolQueue.qsize()).zfill(5)} fQ {str(featureCalculationQueue.qsize()).zfill(5)} rQ        sQ       process {pid} Making miniPool in queue for index {index}')
        miniPool = tradePool.getMiniPool(index, tp.TradePool('mini'), pid)
        while featureCalculationQueue.qsize() > maxQueueSize:
            time.sleep(0.1)
        featureCalculationQueue.put(miniPool)
        # miniPoolList = tradePool.getInbetweenMiniPools(index, tp.TradePool('mini'), pid)
        miniPoolList = []
        logging.debug(f'x{pid} mQ {str(makeMiniPoolQueue.qsize()).zfill(5)} fQ {str(featureCalculationQueue.qsize()).zfill(5)} rQ        sQ       process {pid} Made {len(miniPoolList)} gap miniPools after index {index}')
        makeMiniPoolQueue.task_done()
    makeMiniPoolQueue.task_done()

def featureCalculationWorker(
            featureCalculationQueue,
            featureCalculationProcessCount,
            isLogger,
            makeMiniPoolQueue,
            resultLoggerQueue,
            recordsTotal,
            outputFolder,
            features,
            maxQueueSize
        ):

    pid = multiprocessing.current_process().pid
    logging.info(f'x{pid} Feature calculation worker started {pid}')
    processStart = timing.startCalculation()
    logAfter = 50
    processed = 0
    significant_digits = 8
    lengths = []
    started = time.time()
    while True:
        miniPool = featureCalculationQueue.get()
        if miniPool is None:
            logging.info(f'x{pid} None arrived in featureCalculationWorker')
            break

        lengths.append(miniPool.subPools['past_twoHours']['endIndex'] - miniPool.subPools['past_twoHours']['startIndex'])
        logging.debug(f'x{pid} mQ {str(makeMiniPoolQueue.qsize()).zfill(5)} fQ {str(featureCalculationQueue.qsize()).zfill(5)} rQ {str(resultLoggerQueue.qsize()).zfill(5)} process {pid} Calculating features in queue')
        row = dataCalculate.calculateAllFeaturesToList(miniPool, features, pid)
        rounded_row = [round_to_significant_digits(x, significant_digits) for x in row]
        message = ",".join(f"{x:.{significant_digits}g}" for x in rounded_row)
        resultLoggerQueue.put(message)
        while resultLoggerQueue.qsize() > (maxQueueSize / 4):
            time.sleep(0.1)
        # del miniPool, row

        processed += 1
        if processed % logAfter == 0 and isLogger:
            # logging.info(f'{miniPool.subPools}')
            combinedProcessesCompleted = processed * featureCalculationProcessCount
            timing.endCalculation(processStart, combinedProcessesCompleted, recordsTotal)
            now = time.time()
            elapsed = now - started
            started = now
            event = {}
            timeElapsed = timing.secondsToStr(elapsed, True)
            avgLength = np.average(lengths)
            timePrLength = elapsed / avgLength
            event["time elapsed"] = timeElapsed
            event["avg length"] = avgLength
            event["time pr length"] = timePrLength * 1000
            logging.info(f'Time to Process{event}')
            logging.info(f'x{pid} mQ {str(makeMiniPoolQueue.qsize()).zfill(5)} fQ {str(featureCalculationQueue.qsize()).zfill(5)} rQ {str(resultLoggerQueue.qsize()).zfill(5)} process {pid} Calculating features in queue')
        featureCalculationQueue.task_done()

    logging.info(f'x{pid} Pid complete: {pid}')
    featureCalculationQueue.task_done()

def resultLoggerWorker(resultLoggerQueue, cloudLogger):
    while True:
        message = resultLoggerQueue.get()
        if message is None:
            logging.info(f'None arrived in resultLoggerWorker')
            break
        cloudLogger.log(message)
        resultLoggerQueue.task_done()
    cloudLogger.flush()
    logging.info(f'logger queue complete')
    resultLoggerQueue.task_done()

def round_to_significant_digits(value, digits):
    if value == 0:
        return 0
    else:
        return round(value, digits - int(np.floor(np.log10(abs(value)))) - 1)

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
    tradePool = tp.TradePool('parent')
    return tradePool

def setupFeatures():
    features = f.Features()
    return features

def getPythonPids():
    pythonPids = []
    for proc in psutil.process_iter(['pid', 'name']):
        if 'python' in proc.info['name']:
            pythonPids.append(proc.info['pid'])
    return pythonPids

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
    if "test" not in args.source:
        logging.info("production run ending in shutdown")
        # os.system("shutdown now -h")
    logging.info("test run ending")
