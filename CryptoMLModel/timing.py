import atexit
from time import time, strftime, localtime
from datetime import timedelta
import logging

def secondsToStr(elapsed=None, timeDelta=None):
    if elapsed is None:
        return strftime("%Y-%m-%d %H:%M:%S", localtime())
    if timeDelta is None:
        return f'{str(timedelta(seconds=elapsed)).split(".")[0]}'
    return f'{str(timedelta(seconds=elapsed))}'

def log(s, elapsed=None, perRecord=None, estimatedTotal=None):
    line = "="*40
    logging.info(line)
    logging.info(f'{secondsToStr()} - {s}')
    if elapsed:
        logging.info(f'Elapsed time: {elapsed}')
    if perRecord:
        logging.info(f'Per record time: {perRecord}')
    if estimatedTotal:
        logging.info(f'Estimated total time: {estimatedTotal}')

def now():
    return time()

def startCalculation():
    return time()

def endCalculation(start, recordsInBatch, totalRecords=None):
    elapsed = time() - start
    perRecord = elapsed / recordsInBatch
    if totalRecords:
        estimatedTotal = perRecord * totalRecords
        log('Batch calculation', None, secondsToStr(perRecord, True), secondsToStr(estimatedTotal, True))
        return
    log('Batch calculation', None, secondsToStr(perRecord, True))

def endlog():
    end = time()
    elapsed = end-start
    log("End Program", secondsToStr(elapsed))

def startTiming():
    log("Start Program")

start = time()
atexit.register(endlog)