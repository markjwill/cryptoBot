import mydb
import tradePool
import dataCalculate

selectTrades = """
SELECT price, amount, type, date_ms, trade_id
FROM trades
LIMIT %s
OFFSET ?
ORDER BY date_ms ASC
"""

def main():
    numberOfTrades = 500000
    offset = i * numberOfTrades
    tradeList = mydb.selectAll(selectTrades % str(numberOfTrades), (offset,))
    tradePool = TradePool(tradeList)

    df = dataCalculate.setupDataFrame()
    for trade in tradePool.getTradeList():
        if trade[4] > farthestCompleteTradeId:
            if trade[3] == previousMilliseconds:

                continue
            previousFeatures = calculateAllFeatureGroups(df, tradePool, trade[4], trade[3])
            previousMilliseconds = trade[3]

    # millions of trades have the same date_ms, 
    # we need to not calculate the features multiple times for these

    df.saveToDb() #placeholder

if __name__ == '__main__':
    main()
    print("script end reached")







