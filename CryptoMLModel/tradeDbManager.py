import mydb
import logging
import datetime
import hashlib
import json

class TradeDbManager:

    selectTrades = """
SELECT `price`, `amount`, `type`, `date_ms`, `trades`.`id`, `coinbasePrice`, `huobiPrice` ,`binancePrice`
FROM `trades`
JOIN `outsidePrices` on ( `trades`.`id` = `outsidePrices`.`tradeId` )
WHERE `trades`.`id` > ?
AND `coinbasePrice`!= 0
AND `huobiPrice`!= 0
AND `binancePrice`!= 0
ORDER BY `date_ms` ASC
LIMIT %s, %s
"""

    selectWorkerOffsetTrade = """
SELECT `trades`.`id`
FROM `trades`
JOIN `outsidePrices` on ( `trades`.`id` = `outsidePrices`.`tradeId` )
WHERE `coinbasePrice`!= 0
AND `huobiPrice`!= 0
AND `binancePrice`!= 0
ORDER BY `trades`.`id` ASC
LIMIT %s, %s
"""

    selectFarthestCompleteTrade = """
SELECT `index`
FROM `%s`
WHERE `index` > ?
AND `index` < ?
ORDER BY `index` DESC
LIMIT 1
"""

    selectDateMsById = """
SELECT `date_ms`
FROM `trades`
WHERE `id` = ?
"""

    selectMaxPeriodAgoId = """
SELECT `id`
FROM `trades`
WHERE `date_ms` < ?
ORDER BY `date_ms` DESC
LIMIT 1
"""
    offset = 0
    tradeBatchSize = 30000
    farthestCompleteTradeId = 0
    offsetId = 0
    calculatedTableName = 'tradesCalculated'
    maxTimePeriod = 0
    WorkerStartId = 0
    WorkerEndId = 0
    workerCount = 1
    workerIndex = 0
    APROXIMATE_RECORD_COUNT = 13000000
    MAX_WORKERS = 3

    def __init__(self, workerCount, workerIndex):
        if workerCount > self.MAX_WORKERS:
            raise AssertionError
        if workerIndex >= workerCount:
            raise AssertionError
        self.workerCount = workerCount
        self.workerIndex = workerIndex
        workerNumberOfRecords = int(self.APROXIMATE_RECORD_COUNT / self.workerCount)
        workerStartOffset = workerNumberOfRecords * self.workerIndex
        WorkerEndOffset = workerNumberOfRecords * ( self.workerIndex + 1 )
        self.WorkerStartId, *x = mydb.selectOne(
            self.selectWorkerOffsetTrade % workerStartOffset, 1
        )
        self.WorkerEndId, *x = mydb.selectOne(
            self.selectWorkerOffsetTrade % workerEndOffset, 1
        )

    def getFarthestCompleteTradeId(self):
        try:
            self.farthestCompleteTradeId, *x = mydb.selectOne(
                self.selectFarthestCompleteTrade % self.calculatedTableName,
                (self.WorkerStartId, self.WorkerEndId)
            )
            logging.debug(f'FarthestCompleteTradeId: {self.farthestCompleteTradeId}')
            farthestCompleteTradeMilliseconds, *x = mydb.selectOne(
                self.selectDateMsById, (self.farthestCompleteTradeId, )
            )
            logging.debug(f'FarthestCompleteTradeMilliseconds: {farthestCompleteTradeMilliseconds} date: {self.logTime(farthestCompleteTradeMilliseconds)}')
            targetStartMilliseconds = farthestCompleteTradeMilliseconds - self.maxTimePeriod
            self.offsetId, *x = mydb.selectOne(
                self.selectMaxPeriodAgoId, (targetStartMilliseconds, )
            )
            logging.debug(f'targetMilliseconds: {targetStartMilliseconds} date: {self.logTime(targetStartMilliseconds)}')

        except mydb.mariadb.ProgrammingError as error:
            logging.info('No existing tradesCalculated table found')
            logging.debug('Or some other mysql problem exists:')
            logging.debug(error)
        logging.info(f'Farthest complete tradeId {self.farthestCompleteTradeId} OffsetId set to {self.offsetId}')

        return self.farthestCompleteTradeId

    def getStarterTradeList(self):
        logging.info('Starter Trade List requested')
        return self.getTradeList(3)

    def getAdditionalTradeList(self, batchMultiplier):
        logging.info('Additional Trade List requested')
        return self.getTradeList(batchMultiplier)

    def getTradeList(self, batchCount):
        limit = int(self.tradeBatchSize * batchCount)
        logging.debug('More Trades Query:'
            f' {self.selectTrades % (str(self.offset), str(limit))}'
            f' with offset {self.offsetId}.')
        tradeList = mydb.selectAll(self.selectTrades % (str(self.offset), str(limit)), (self.offsetId,))
        self.offset = self.offset + limit
        firstTradeDatetime = datetime.datetime.fromtimestamp(tradeList[0][3]/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        lastTradeDatetime = datetime.datetime.fromtimestamp(tradeList[-1][3]/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        logging.debug(f'Trade List from {firstTradeDatetime} to {lastTradeDatetime} with limit {limit} collected {len(tradeList)} trades.')
        return tradeList

    def logTime(self, milliseconds):
        return datetime.datetime.fromtimestamp(milliseconds/1000.0).strftime('%Y-%m-%d %H:%M:%S')

    def setMaxTimePeriod(self, maxTimePeriod):
        self.maxTimePeriod = maxTimePeriod

    def getUniqueTableName(self, periodsAndfeatures):
        dhash = hashlib.md5()
        encoded = json.dumps(periodsAndfeatures, sort_keys=True).encode()
        dhash.update(encoded)
        self.calculatedTableName = f'tradesCalculated_{dhash.hexdigest()}'
        return self.calculatedTableName

    def logTime(self, milliseconds):
        return datetime.datetime.fromtimestamp(milliseconds/1000.0).strftime('%Y-%m-%d %H:%M:%S')


