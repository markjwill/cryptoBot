import sys
import mydb as db
import numpy as np
import dataNormalization as dn
from timeit import default_timer as timer
from datetime import timedelta

selectCols = "prepedTrades.trade_id, prepedTrades.seconds_in_day_sin, prepedTrades.seconds_in_day_cos, prepedTrades.day_in_week_sin, prepedTrades.day_in_week_cos, prepedTrades.day_in_year_sin, prepedTrades.day_in_year_cos, prepedTrades.amount_traded, prepedTrades.type, prepedTrades.p_fiveSec_avgPrice, prepedTrades.p_fiveSec_highPrice, prepedTrades.p_fiveSec_lowPrice, prepedTrades.p_fiveSec_startPrice, prepedTrades.p_fiveSec_endPrice, prepedTrades.p_fiveSec_changeReal, prepedTrades.p_fiveSec_changePercent, prepedTrades.p_fiveSec_travelReal, prepedTrades.p_fiveSec_travelPercent, prepedTrades.p_fiveSec_volumePrMin, prepedTrades.p_fiveSec_tradesPrMin, prepedTrades.p_fiveSec_buysPrMin, prepedTrades.p_fiveSec_sellsPrMin, prepedTrades.p_tenSec_avgPrice, prepedTrades.p_tenSec_highPrice, prepedTrades.p_tenSec_lowPrice, prepedTrades.p_tenSec_startPrice, prepedTrades.p_tenSec_endPrice, prepedTrades.p_tenSec_changeReal, prepedTrades.p_tenSec_changePercent, prepedTrades.p_tenSec_travelReal, prepedTrades.p_tenSec_travelPercent, prepedTrades.p_tenSec_volumePrMin, prepedTrades.p_tenSec_tradesPrMin, prepedTrades.p_tenSec_buysPrMin, prepedTrades.p_tenSec_sellsPrMin, prepedTrades.p_thirtySec_avgPrice, prepedTrades.p_thirtySec_highPrice, prepedTrades.p_thirtySec_lowPrice, prepedTrades.p_thirtySec_startPrice, prepedTrades.p_thirtySec_endPrice, prepedTrades.p_thirtySec_changeReal, prepedTrades.p_thirtySec_changePercent, prepedTrades.p_thirtySec_travelReal, prepedTrades.p_thirtySec_travelPercent, prepedTrades.p_thirtySec_volumePrMin, prepedTrades.p_thirtySec_tradesPrMin, prepedTrades.p_thirtySec_buysPrMin, prepedTrades.p_thirtySec_sellsPrMin, prepedTrades.p_oneMin_avgPrice, prepedTrades.p_oneMin_highPrice, prepedTrades.p_oneMin_lowPrice, prepedTrades.p_oneMin_startPrice, prepedTrades.p_oneMin_endPrice, prepedTrades.p_oneMin_changeReal, prepedTrades.p_oneMin_changePercent, prepedTrades.p_oneMin_travelReal, prepedTrades.p_oneMin_travelPercent, prepedTrades.p_oneMin_volumePrMin, prepedTrades.p_oneMin_tradesPrMin, prepedTrades.p_oneMin_buysPrMin, prepedTrades.p_oneMin_sellsPrMin, prepedTrades.p_threeMin_avgPrice, prepedTrades.p_threeMin_highPrice, prepedTrades.p_threeMin_lowPrice, prepedTrades.p_threeMin_startPrice, prepedTrades.p_threeMin_endPrice, prepedTrades.p_threeMin_changeReal, prepedTrades.p_threeMin_changePercent, prepedTrades.p_threeMin_travelReal, prepedTrades.p_threeMin_travelPercent, prepedTrades.p_threeMin_volumePrMin, prepedTrades.p_threeMin_tradesPrMin, prepedTrades.p_threeMin_buysPrMin, prepedTrades.p_threeMin_sellsPrMin, prepedTrades.p_fiveMin_avgPrice, prepedTrades.p_fiveMin_highPrice, prepedTrades.p_fiveMin_lowPrice, prepedTrades.p_fiveMin_startPrice, prepedTrades.p_fiveMin_endPrice, prepedTrades.p_fiveMin_changeReal, prepedTrades.p_fiveMin_changePercent, prepedTrades.p_fiveMin_travelReal, prepedTrades.p_fiveMin_travelPercent, prepedTrades.p_fiveMin_volumePrMin, prepedTrades.p_fiveMin_tradesPrMin, prepedTrades.p_fiveMin_buysPrMin, prepedTrades.p_fiveMin_sellsPrMin, prepedTrades.p_tenMin_avgPrice, prepedTrades.p_tenMin_highPrice, prepedTrades.p_tenMin_lowPrice, prepedTrades.p_tenMin_startPrice, prepedTrades.p_tenMin_endPrice, prepedTrades.p_tenMin_changeReal, prepedTrades.p_tenMin_changePercent, prepedTrades.p_tenMin_travelReal, prepedTrades.p_tenMin_travelPercent, prepedTrades.p_tenMin_volumePrMin, prepedTrades.p_tenMin_tradesPrMin, prepedTrades.p_tenMin_buysPrMin, prepedTrades.p_tenMin_sellsPrMin, prepedTrades.p_fifteenMin_avgPrice, prepedTrades.p_fifteenMin_highPrice, prepedTrades.p_fifteenMin_lowPrice, prepedTrades.p_fifteenMin_startPrice, prepedTrades.p_fifteenMin_endPrice, prepedTrades.p_fifteenMin_changeReal, prepedTrades.p_fifteenMin_changePercent, prepedTrades.p_fifteenMin_travelReal, prepedTrades.p_fifteenMin_travelPercent, prepedTrades.p_fifteenMin_volumePrMin, prepedTrades.p_fifteenMin_tradesPrMin, prepedTrades.p_fifteenMin_buysPrMin, prepedTrades.p_fifteenMin_sellsPrMin, prepedTrades.p_thirtyMin_avgPrice, prepedTrades.p_thirtyMin_highPrice, prepedTrades.p_thirtyMin_lowPrice, prepedTrades.p_thirtyMin_startPrice, prepedTrades.p_thirtyMin_endPrice, prepedTrades.p_thirtyMin_changeReal, prepedTrades.p_thirtyMin_changePercent, prepedTrades.p_thirtyMin_travelReal, prepedTrades.p_thirtyMin_travelPercent, prepedTrades.p_thirtyMin_volumePrMin, prepedTrades.p_thirtyMin_tradesPrMin, prepedTrades.p_thirtyMin_buysPrMin, prepedTrades.p_thirtyMin_sellsPrMin, prepedTrades.p_sixtyMin_avgPrice, prepedTrades.p_sixtyMin_highPrice, prepedTrades.p_sixtyMin_lowPrice, prepedTrades.p_sixtyMin_startPrice, prepedTrades.p_sixtyMin_endPrice, prepedTrades.p_sixtyMin_changeReal, prepedTrades.p_sixtyMin_changePercent, prepedTrades.p_sixtyMin_travelReal, prepedTrades.p_sixtyMin_travelPercent, prepedTrades.p_sixtyMin_volumePrMin, prepedTrades.p_sixtyMin_tradesPrMin, prepedTrades.p_sixtyMin_buysPrMin, prepedTrades.p_sixtyMin_sellsPrMin, prepedTrades.p_oneTwentyMin_avgPrice, prepedTrades.p_oneTwentyMin_highPrice, prepedTrades.p_oneTwentyMin_lowPrice, prepedTrades.p_oneTwentyMin_startPrice, prepedTrades.p_oneTwentyMin_endPrice, prepedTrades.p_oneTwentyMin_changeReal, prepedTrades.p_oneTwentyMin_changePercent, prepedTrades.p_oneTwentyMin_travelReal, prepedTrades.p_oneTwentyMin_travelPercent, prepedTrades.p_oneTwentyMin_volumePrMin, prepedTrades.p_oneTwentyMin_tradesPrMin, prepedTrades.p_oneTwentyMin_buysPrMin, prepedTrades.p_oneTwentyMin_sellsPrMin, prepedTrades.coinbase_price, prepedTrades.binance_price, prepedTrades.huobi_price, prepedTrades.bittrex_price, prepedTrades.bch_price, prepedTrades.eth_price, prepedTrades.f_fiveSec_endPrice, prepedTrades.f_thirtySec_endPrice, prepedTrades.f_fifteenMin_endPrice, prepedTrades.f_oneTwentyMin_endPrice"
insertCols = "trade_id, seconds_in_day_sin, seconds_in_day_cos, day_in_week_sin, day_in_week_cos, day_in_year_sin, day_in_year_cos, amount_traded, type, p_fiveSec_avgPrice, p_fiveSec_highPrice, p_fiveSec_lowPrice, p_fiveSec_startPrice, p_fiveSec_endPrice, p_fiveSec_changeReal, p_fiveSec_changePercent, p_fiveSec_travelReal, p_fiveSec_travelPercent, p_fiveSec_volumePrMin, p_fiveSec_tradesPrMin, p_fiveSec_buysPrMin, p_fiveSec_sellsPrMin, p_tenSec_avgPrice, p_tenSec_highPrice, p_tenSec_lowPrice, p_tenSec_startPrice, p_tenSec_endPrice, p_tenSec_changeReal, p_tenSec_changePercent, p_tenSec_travelReal, p_tenSec_travelPercent, p_tenSec_volumePrMin, p_tenSec_tradesPrMin, p_tenSec_buysPrMin, p_tenSec_sellsPrMin, p_thirtySec_avgPrice, p_thirtySec_highPrice, p_thirtySec_lowPrice, p_thirtySec_startPrice, p_thirtySec_endPrice, p_thirtySec_changeReal, p_thirtySec_changePercent, p_thirtySec_travelReal, p_thirtySec_travelPercent, p_thirtySec_volumePrMin, p_thirtySec_tradesPrMin, p_thirtySec_buysPrMin, p_thirtySec_sellsPrMin, p_oneMin_avgPrice, p_oneMin_highPrice, p_oneMin_lowPrice, p_oneMin_startPrice, p_oneMin_endPrice, p_oneMin_changeReal, p_oneMin_changePercent, p_oneMin_travelReal, p_oneMin_travelPercent, p_oneMin_volumePrMin, p_oneMin_tradesPrMin, p_oneMin_buysPrMin, p_oneMin_sellsPrMin, p_threeMin_avgPrice, p_threeMin_highPrice, p_threeMin_lowPrice, p_threeMin_startPrice, p_threeMin_endPrice, p_threeMin_changeReal, p_threeMin_changePercent, p_threeMin_travelReal, p_threeMin_travelPercent, p_threeMin_volumePrMin, p_threeMin_tradesPrMin, p_threeMin_buysPrMin, p_threeMin_sellsPrMin, p_fiveMin_avgPrice, p_fiveMin_highPrice, p_fiveMin_lowPrice, p_fiveMin_startPrice, p_fiveMin_endPrice, p_fiveMin_changeReal, p_fiveMin_changePercent, p_fiveMin_travelReal, p_fiveMin_travelPercent, p_fiveMin_volumePrMin, p_fiveMin_tradesPrMin, p_fiveMin_buysPrMin, p_fiveMin_sellsPrMin, p_tenMin_avgPrice, p_tenMin_highPrice, p_tenMin_lowPrice, p_tenMin_startPrice, p_tenMin_endPrice, p_tenMin_changeReal, p_tenMin_changePercent, p_tenMin_travelReal, p_tenMin_travelPercent, p_tenMin_volumePrMin, p_tenMin_tradesPrMin, p_tenMin_buysPrMin, p_tenMin_sellsPrMin, p_fifteenMin_avgPrice, p_fifteenMin_highPrice, p_fifteenMin_lowPrice, p_fifteenMin_startPrice, p_fifteenMin_endPrice, p_fifteenMin_changeReal, p_fifteenMin_changePercent, p_fifteenMin_travelReal, p_fifteenMin_travelPercent, p_fifteenMin_volumePrMin, p_fifteenMin_tradesPrMin, p_fifteenMin_buysPrMin, p_fifteenMin_sellsPrMin, p_thirtyMin_avgPrice, p_thirtyMin_highPrice, p_thirtyMin_lowPrice, p_thirtyMin_startPrice, p_thirtyMin_endPrice, p_thirtyMin_changeReal, p_thirtyMin_changePercent, p_thirtyMin_travelReal, p_thirtyMin_travelPercent, p_thirtyMin_volumePrMin, p_thirtyMin_tradesPrMin, p_thirtyMin_buysPrMin, p_thirtyMin_sellsPrMin, p_sixtyMin_avgPrice, p_sixtyMin_highPrice, p_sixtyMin_lowPrice, p_sixtyMin_startPrice, p_sixtyMin_endPrice, p_sixtyMin_changeReal, p_sixtyMin_changePercent, p_sixtyMin_travelReal, p_sixtyMin_travelPercent, p_sixtyMin_volumePrMin, p_sixtyMin_tradesPrMin, p_sixtyMin_buysPrMin, p_sixtyMin_sellsPrMin, p_oneTwentyMin_avgPrice, p_oneTwentyMin_highPrice, p_oneTwentyMin_lowPrice, p_oneTwentyMin_startPrice, p_oneTwentyMin_endPrice, p_oneTwentyMin_changeReal, p_oneTwentyMin_changePercent, p_oneTwentyMin_travelReal, p_oneTwentyMin_travelPercent, p_oneTwentyMin_volumePrMin, p_oneTwentyMin_tradesPrMin, p_oneTwentyMin_buysPrMin, p_oneTwentyMin_sellsPrMin, coinbase_price, binance_price, huobi_price, bittrex_price, bch_price, eth_price, f_fiveSec_endPrice, f_thirtySec_endPrice, f_fifteenMin_endPrice, f_oneTwentyMin_endPrice"

selectForBuild = """
SELECT %s
FROM prepedTrades
ORDER BY trade_id DESC
LIMIT 1000
OFFSET ?"""

# Column 'trade_id' in field list is ambiguous
selectForNorm = """
SELECT %s 
FROM prepedTrades 
LEFT JOIN normedTrades on ( normedTrades.trade_id = prepedTrades.trade_id )
WHERE normedTrades.trade_id IS NULL
ORDER BY trade_id DESC
LIMIT 1000 
OFFSET ?"""

insertNormed = """
INSERT INTO normedTrades (%s) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

def buildScaler():
  start = timer()
  i = 0
  while True:
  # while True:
    offset = (i * 1000)
    print("build scaler OFFSET: %s"% offset)
    i += 1
    results = db.selectAll(selectForBuild % selectCols, (offset,))

    if len(results) == 0:
      break
    # print("Results:", results[0])
    array = np.array(results)
    # print("Data: ", array)
    dn.batchScalerBuild(array, dn.scaler, dn.scalerFileName)
    end = timer()
    print("elapsed: ",timedelta(seconds=end-start))

def normalizeAndSave():
  i = 0
  while True:
    start = timer()
    offset = (i * 1000)
    i += 1
    results = db.selectAll(selectForNorm % selectCols, (offset,))

    if len(results) == 0:
      break
    array = np.array(results)
    normed = dn.normTrades(array, dn.scaler)
    insertData = list(normed)

    for a in range(len(insertData)):
      insertData[a] = list(insertData[a])
      insertData[a][0] = results[a][0]
      insertData[a] = tuple(insertData[a])

    end = timer()
    print("elapsed: ",timedelta(seconds=end-start))
    db.insertMany(insertNormed % insertCols, insertData )
    print("finished OFFSET: %s"% offset)
    end = timer()



def scriptError():
  print("No action taken, please provide a function for this script to perform.")
  print("  options are:")
  print("    -build     This option will red cleaned data and build a sklearn MaxAbsScaler scaler based on the data.")
  print("    -norm      This option will normalize cleaned data and save it to the database.")
  exit()

if len(sys.argv) == 1:
  scriptError()
if sys.argv[1] == "-build":
  buildScaler()
elif  sys.argv[1] == "-norm":
  normalizeAndSave()
else:
  scriptError()

print("script end reached")





