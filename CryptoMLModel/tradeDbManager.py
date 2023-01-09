import mydb
import logging
import datetime
import hashlib
import json

class TradeDbManager:

    selectTrades = """
SELECT `price`, `amount`, `type`, `date_ms`, `id`
FROM `trades`
WHERE `id` > ?
ORDER BY `date_ms` ASC
LIMIT %s, %s
"""

    selectFarthestCompleteTrade = """
SELECT `index`
FROM `%s`
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

    def getFarthestCompleteTradeId(self):
        try:
            self.farthestCompleteTradeId, *x = mydb.selectOneStatic(
                self.selectFarthestCompleteTrade % self.calculatedTableName
            )
            farthestCompleteTradeMilliseconds, *x = mydb.selectOne(
                self.selectDateMsById, self.farthestCompleteTradeId
            )
            self.offsetId, *x = mydb.selectOne(
                self.selectMaxPeriodAgoId, (
                    farthestCompleteTradeMilliseconds - self.maxTimePeriod
                )
            )
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

    def getUniqueTableName(self, periods, features):
        combinedDictionary = periods | features
        dhash = hashlib.md5()
        encoded = json.dumps(combinedDictionary, sort_keys=True).encode()
        dhash.update(encoded)
        self.calculatedTableName = f'tradesCalculated_{dhash.hexdigest()}'
        return self.calculatedTableName



