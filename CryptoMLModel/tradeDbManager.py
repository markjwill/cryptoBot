import mydb
import logging
import datetime

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
LIMIT 1
ORDER BY `trade_id` DESC
"""

    offset = 0
    tradeBatchSize = 30000
    farthestCompleteTradeId = 0
    offsetId = 0
    calculatedTableName = 'tradesCalculated'

    def setCalculatedTableName(self, tableName):
        self.calculatedTableName = tableName

    def getFarthestCompleteTradeId(self):
        try:
            self.farthestCompleteTradeId = mydb.selectOneStatic(
                self.selectFarthestCompleteTrade % self.calculatedTableName
            )
            self.offsetId = self.farthestCompleteTradeId - self.tradeBatchSize
        except mydb.mariadb.ProgrammingError as error:
            logging.info('No existing tradesCalculated table found')
        logging.info(f'Farthest complete tradeId {self.farthestCompleteTradeId}')
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
