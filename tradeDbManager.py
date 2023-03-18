import mydb
import logging
import datetime
import hashlib
import json

class TradeDbManager:

# ORDER BY removed and LIMIT in place for debug only
  selectTrades = """
SELECT `price`, `amount`, IF(`type` = 'buy', 1, -1) as 'type', `date_ms`, `id`, `coinbasePrice`, `huobiPrice` ,`binancePrice`
FROM `tradeData`
ORDER BY `date_ms` ASC
-- LIMIT 750000
"""

  offset = 0
  tradeBatchSize = 30000
  offsetId = 0
  APROXIMATE_RECORD_COUNT = 13371000
  recordCount = 0

  def getStarterTradeList(self):
    logging.info('Starter Trade List requested')
    return self.getTradeTuple(3)

  def getAdditionalTradeList(self, batchMultiplier):
    logging.info('Additional Trade List requested')
    return self.getTradeTuple(batchMultiplier)

  def getTradeTuple(self, batchCount):
    limit = int(self.tradeBatchSize * batchCount)
    tradeList = mydb.selectAllStatic(self.selectTrades)
    if not tradeList:
      raise StopIteration('0 trades returned from DB')
    self.offset = self.offset + limit
    firstTradeDatetime = datetime.datetime.fromtimestamp(tradeList[0][3]/1000.0).strftime('%Y-%m-%d %H:%M:%S')
    lastTradeDatetime = datetime.datetime.fromtimestamp(tradeList[-1][3]/1000.0).strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f'Trade List from {firstTradeDatetime} to {lastTradeDatetime} with limit {limit} collected {len(tradeList)} trades.')
    self.recordCount = len(tradeList)
    return tuple(tradeList)

  def logTime(self, milliseconds):
    return datetime.datetime.fromtimestamp(milliseconds/1000.0).strftime('%Y-%m-%d %H:%M:%S')

  def logTime(self, milliseconds):
    return datetime.datetime.fromtimestamp(milliseconds/1000.0).strftime('%Y-%m-%d %H:%M:%S')


