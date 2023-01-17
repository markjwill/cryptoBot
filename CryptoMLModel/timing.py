import atexit
from time import time, strftime, localtime
from datetime import timedelta
import logging

def secondsToStr(elapsed=None, perRecord=None):
    if elapsed is None:
        return strftime("%Y-%m-%d %H:%M:%S", localtime())
    if perRecord is None:
        return f'{str(timedelta(seconds=elapsed)).split(".")[0]}'
    return f'{str(timedelta(seconds=elapsed))}'

def log(s, elapsed=None, perRecord=None):
    line = "="*40
    logging.info(line)
    logging.info(f'{secondsToStr()} - {s}')
    if elapsed:
        logging.info(f'Elapsed time: {elapsed}')
    if perRecord:
        logging.info(f'Per record time: {perRecord}')

def now():
    return time()

def startCalculation():
    return time()

def endCalculation(start, recordsInBatch):
    elapsed = time() - start
    perRecord = elapsed / recordsInBatch
    log('Batch calculation complete', None, secondsToStr(perRecord, True))

def endlog():
    end = time()
    elapsed = end-start
    log("End Program", secondsToStr(elapsed))

def startTiming():
    log("Start Program")

start = time()
atexit.register(endlog)