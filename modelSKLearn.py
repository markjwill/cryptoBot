import mydb as db
import numpy as np
import sys
import joblib
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_validate
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
import dataNormalization as dn
import dataPrep as dp
import matplotlib
matplotlib.use('TkCairo')
import matplotlib.pyplot as plt

def main():
    selectCols = "trade_id, seconds_in_day_sin, seconds_in_day_cos, day_in_week_sin, day_in_week_cos, day_in_year_sin, day_in_year_cos, amount_traded, type, p_fiveSec_avgPrice, p_fiveSec_highPrice, p_fiveSec_lowPrice, p_fiveSec_startPrice, p_fiveSec_endPrice, p_fiveSec_changeReal, p_fiveSec_changePercent, p_fiveSec_travelReal, p_fiveSec_travelPercent, p_fiveSec_volumePrMin, p_fiveSec_tradesPrMin, p_fiveSec_buysPrMin, p_fiveSec_sellsPrMin, p_tenSec_avgPrice, p_tenSec_highPrice, p_tenSec_lowPrice, p_tenSec_startPrice, p_tenSec_endPrice, p_tenSec_changeReal, p_tenSec_changePercent, p_tenSec_travelReal, p_tenSec_travelPercent, p_tenSec_volumePrMin, p_tenSec_tradesPrMin, p_tenSec_buysPrMin, p_tenSec_sellsPrMin, p_thirtySec_avgPrice, p_thirtySec_highPrice, p_thirtySec_lowPrice, p_thirtySec_startPrice, p_thirtySec_endPrice, p_thirtySec_changeReal, p_thirtySec_changePercent, p_thirtySec_travelReal, p_thirtySec_travelPercent, p_thirtySec_volumePrMin, p_thirtySec_tradesPrMin, p_thirtySec_buysPrMin, p_thirtySec_sellsPrMin, p_oneMin_avgPrice, p_oneMin_highPrice, p_oneMin_lowPrice, p_oneMin_startPrice, p_oneMin_endPrice, p_oneMin_changeReal, p_oneMin_changePercent, p_oneMin_travelReal, p_oneMin_travelPercent, p_oneMin_volumePrMin, p_oneMin_tradesPrMin, p_oneMin_buysPrMin, p_oneMin_sellsPrMin, p_threeMin_avgPrice, p_threeMin_highPrice, p_threeMin_lowPrice, p_threeMin_startPrice, p_threeMin_endPrice, p_threeMin_changeReal, p_threeMin_changePercent, p_threeMin_travelReal, p_threeMin_travelPercent, p_threeMin_volumePrMin, p_threeMin_tradesPrMin, p_threeMin_buysPrMin, p_threeMin_sellsPrMin, p_fiveMin_avgPrice, p_fiveMin_highPrice, p_fiveMin_lowPrice, p_fiveMin_startPrice, p_fiveMin_endPrice, p_fiveMin_changeReal, p_fiveMin_changePercent, p_fiveMin_travelReal, p_fiveMin_travelPercent, p_fiveMin_volumePrMin, p_fiveMin_tradesPrMin, p_fiveMin_buysPrMin, p_fiveMin_sellsPrMin, p_tenMin_avgPrice, p_tenMin_highPrice, p_tenMin_lowPrice, p_tenMin_startPrice, p_tenMin_endPrice, p_tenMin_changeReal, p_tenMin_changePercent, p_tenMin_travelReal, p_tenMin_travelPercent, p_tenMin_volumePrMin, p_tenMin_tradesPrMin, p_tenMin_buysPrMin, p_tenMin_sellsPrMin, p_fifteenMin_avgPrice, p_fifteenMin_highPrice, p_fifteenMin_lowPrice, p_fifteenMin_startPrice, p_fifteenMin_endPrice, p_fifteenMin_changeReal, p_fifteenMin_changePercent, p_fifteenMin_travelReal, p_fifteenMin_travelPercent, p_fifteenMin_volumePrMin, p_fifteenMin_tradesPrMin, p_fifteenMin_buysPrMin, p_fifteenMin_sellsPrMin, p_thirtyMin_avgPrice, p_thirtyMin_highPrice, p_thirtyMin_lowPrice, p_thirtyMin_startPrice, p_thirtyMin_endPrice, p_thirtyMin_changeReal, p_thirtyMin_changePercent, p_thirtyMin_travelReal, p_thirtyMin_travelPercent, p_thirtyMin_volumePrMin, p_thirtyMin_tradesPrMin, p_thirtyMin_buysPrMin, p_thirtyMin_sellsPrMin, p_sixtyMin_avgPrice, p_sixtyMin_highPrice, p_sixtyMin_lowPrice, p_sixtyMin_startPrice, p_sixtyMin_endPrice, p_sixtyMin_changeReal, p_sixtyMin_changePercent, p_sixtyMin_travelReal, p_sixtyMin_travelPercent, p_sixtyMin_volumePrMin, p_sixtyMin_tradesPrMin, p_sixtyMin_buysPrMin, p_sixtyMin_sellsPrMin, p_oneTwentyMin_avgPrice, p_oneTwentyMin_highPrice, p_oneTwentyMin_lowPrice, p_oneTwentyMin_startPrice, p_oneTwentyMin_endPrice, p_oneTwentyMin_changeReal, p_oneTwentyMin_changePercent, p_oneTwentyMin_travelReal, p_oneTwentyMin_travelPercent, p_oneTwentyMin_volumePrMin, p_oneTwentyMin_tradesPrMin, p_oneTwentyMin_buysPrMin, p_oneTwentyMin_sellsPrMin, coinbase_price, binance_price, huobi_price, bittrex_price, bch_price, eth_price, f_fiveSec_endPrice, f_thirtySec_endPrice, f_fifteenMin_endPrice, f_oneTwentyMin_endPrice"
    selectOriginalCols = "trades.id, trades.amount, trades.date, trades.price, trades.type, trades.p_fiveSec_avgPrice, trades.p_fiveSec_highPrice, trades.p_fiveSec_lowPrice, trades.p_fiveSec_startPrice, trades.p_fiveSec_endPrice, trades.p_fiveSec_changeReal, trades.p_fiveSec_changePercent, trades.p_fiveSec_travelReal, trades.p_fiveSec_travelPercent, trades.p_fiveSec_volumePrMin, trades.p_fiveSec_tradesPrMin, trades.p_fiveSec_buysPrMin, trades.p_fiveSec_sellsPrMin, trades.p_tenSec_avgPrice, trades.p_tenSec_highPrice, trades.p_tenSec_lowPrice, trades.p_tenSec_startPrice, trades.p_tenSec_endPrice, trades.p_tenSec_changeReal, trades.p_tenSec_changePercent, trades.p_tenSec_travelReal, trades.p_tenSec_travelPercent, trades.p_tenSec_volumePrMin, trades.p_tenSec_tradesPrMin, trades.p_tenSec_buysPrMin, trades.p_tenSec_sellsPrMin, trades.p_thirtySec_avgPrice, trades.p_thirtySec_highPrice, trades.p_thirtySec_lowPrice, trades.p_thirtySec_startPrice, trades.p_thirtySec_endPrice, trades.p_thirtySec_changeReal, trades.p_thirtySec_changePercent, trades.p_thirtySec_travelReal, trades.p_thirtySec_travelPercent, trades.p_thirtySec_volumePrMin, trades.p_thirtySec_tradesPrMin, trades.p_thirtySec_buysPrMin, trades.p_thirtySec_sellsPrMin, trades.p_oneMin_avgPrice, trades.p_oneMin_highPrice, trades.p_oneMin_lowPrice, trades.p_oneMin_startPrice, trades.p_oneMin_endPrice, trades.p_oneMin_changeReal, trades.p_oneMin_changePercent, trades.p_oneMin_travelReal, trades.p_oneMin_travelPercent, trades.p_oneMin_volumePrMin, trades.p_oneMin_tradesPrMin, trades.p_oneMin_buysPrMin, trades.p_oneMin_sellsPrMin, trades.p_threeMin_avgPrice, trades.p_threeMin_highPrice, trades.p_threeMin_lowPrice, trades.p_threeMin_startPrice, trades.p_threeMin_endPrice, trades.p_threeMin_changeReal, trades.p_threeMin_changePercent, trades.p_threeMin_travelReal, trades.p_threeMin_travelPercent, trades.p_threeMin_volumePrMin, trades.p_threeMin_tradesPrMin, trades.p_threeMin_buysPrMin, trades.p_threeMin_sellsPrMin, trades.p_fiveMin_avgPrice, trades.p_fiveMin_highPrice, trades.p_fiveMin_lowPrice, trades.p_fiveMin_startPrice, trades.p_fiveMin_endPrice, trades.p_fiveMin_changeReal, trades.p_fiveMin_changePercent, trades.p_fiveMin_travelReal, trades.p_fiveMin_travelPercent, trades.p_fiveMin_volumePrMin, trades.p_fiveMin_tradesPrMin, trades.p_fiveMin_buysPrMin, trades.p_fiveMin_sellsPrMin, trades.p_tenMin_avgPrice, trades.p_tenMin_highPrice, trades.p_tenMin_lowPrice, trades.p_tenMin_startPrice, trades.p_tenMin_endPrice, trades.p_tenMin_changeReal, trades.p_tenMin_changePercent, trades.p_tenMin_travelReal, trades.p_tenMin_travelPercent, trades.p_tenMin_volumePrMin, trades.p_tenMin_tradesPrMin, trades.p_tenMin_buysPrMin, trades.p_tenMin_sellsPrMin, trades.p_fifteenMin_avgPrice, trades.p_fifteenMin_highPrice, trades.p_fifteenMin_lowPrice, trades.p_fifteenMin_startPrice, trades.p_fifteenMin_endPrice, trades.p_fifteenMin_changeReal, trades.p_fifteenMin_changePercent, trades.p_fifteenMin_travelReal, trades.p_fifteenMin_travelPercent, trades.p_fifteenMin_volumePrMin, trades.p_fifteenMin_tradesPrMin, trades.p_fifteenMin_buysPrMin, trades.p_fifteenMin_sellsPrMin, trades.p_thirtyMin_avgPrice, trades.p_thirtyMin_highPrice, trades.p_thirtyMin_lowPrice, trades.p_thirtyMin_startPrice, trades.p_thirtyMin_endPrice, trades.p_thirtyMin_changeReal, trades.p_thirtyMin_changePercent, trades.p_thirtyMin_travelReal, trades.p_thirtyMin_travelPercent, trades.p_thirtyMin_volumePrMin, trades.p_thirtyMin_tradesPrMin, trades.p_thirtyMin_buysPrMin, trades.p_thirtyMin_sellsPrMin, trades.p_sixtyMin_avgPrice, trades.p_sixtyMin_highPrice, trades.p_sixtyMin_lowPrice, trades.p_sixtyMin_startPrice, trades.p_sixtyMin_endPrice, trades.p_sixtyMin_changeReal, trades.p_sixtyMin_changePercent, trades.p_sixtyMin_travelReal, trades.p_sixtyMin_travelPercent, trades.p_sixtyMin_volumePrMin, trades.p_sixtyMin_tradesPrMin, trades.p_sixtyMin_buysPrMin, trades.p_sixtyMin_sellsPrMin, trades.p_oneTwentyMin_avgPrice, trades.p_oneTwentyMin_highPrice, trades.p_oneTwentyMin_lowPrice, trades.p_oneTwentyMin_startPrice, trades.p_oneTwentyMin_endPrice, trades.p_oneTwentyMin_changeReal, trades.p_oneTwentyMin_changePercent, trades.p_oneTwentyMin_travelReal, trades.p_oneTwentyMin_travelPercent, trades.p_oneTwentyMin_volumePrMin, trades.p_oneTwentyMin_tradesPrMin, trades.p_oneTwentyMin_buysPrMin, trades.p_oneTwentyMin_sellsPrMin, 'outsidePrices.coinbasePrice', 'outsidePrices.binancePrice', 'outsidePrices.huobiPrice', 'outsidePrices.bittrexPrice', 'outsidePrices.BCHprice', 'outsidePrices.ETHprice', trades.f_fiveSec_endPrice, trades.f_thirtySec_endPrice, trades.f_fifteenMin_endPrice, trades.f_oneTwentyMin_endPrice"

    select = """
    SELECT %s
    FROM normedTrades
    ORDER BY RAND()
    LIMIT 130409
    """
    # 

    selectOriginalTrade = """
    SELECT %s FROM trades
    WHERE trades.id = ?
    """

    results = db.selectAllStatic(select % selectCols)
    array = np.array(results)

    X = array[:,1:155]
    Y5 = array[:,158]
    Y30 = array[:,159]
    Y15 = array[:,160]
    Y120 = array[:,161]

    trade1 = list(results[10])
    # print(trade1)
    # print(X[1])
    # print(Y5[1])
    # print(Y30[1])
    # print(Y15[1])
    # print(Y120[1])

    print("Data ready")

    # train(X,Y5, 154, 158, 157, trade1)
    # train(X,Y30, 155, 159, 158, trade1)
    # train(X,Y15, 156, 160, 159, trade1)
    train(X,Y120, 157, 161, 160, trade1)



def train(X, y, originalKey, predictedKey, normedKey, trade1):
    x_train, x_test, y_train, y_test = train_test_split(X, y, test_size = 0.05, random_state = 42)
    # creating an object of LinearRegression class
    LR = LinearRegression()
    # fitting the training data
    LR.fit(x_train,y_train)

    print("Fit complete")

    y_pred =  LR.predict(x_test)
    score=r2_score(y_test,y_pred)
    print("Y5 score is ",score)
    print("mean_sqrd_error is==",mean_squared_error(y_test,y_pred))
    print("root_mean_squared error of is==",np.sqrt(mean_squared_error(y_test,y_pred)))
    print("trade1")
    print(trade1[0])
    # print(len(trade1))
    result = db.selectAll(selectOriginalTrade % selectOriginalCols, (trade1[0],))
    originalTrade = result[0]
    # print("result")
    # print(result)
    # print("OriginalTrade")
    # print(originalTrade)
    # print(len(originalTrade))
    predicedValue = LR.predict([trade1[1:155]])
    # print("predicedValue")
    # print(predicedValue)
    # print(predicedValue.shape)
    predicedTrade = np.zeros((1,162))
    predicedTrade[0][predictedKey] = predicedValue
    # print("predicedTrade")
    # print(predicedTrade)
    # print(predicedTrade.shape)
    deNormedTrade = dn.deNormTrade(predicedTrade, dn.scaler)
    # print("deNormedTrade")
    # print(deNormedTrade)
    # print(deNormedTrade.shape)
    unPrepedTrade = dp.unprepY(deNormedTrade[0], originalTrade)
    # print("unPrepedTrade")
    # print(unPrepedTrade)
    # print(unPrepedTrade.shape)
    # Make predictions using the testing set

    plt.plot([-1, 1], [-1, 1], 'bo', linestyle="--")
    plt.scatter(y_test,y_pred)
    plt.grid(True)
    plt.xlabel('Actual')
    plt.ylabel('Predicted')
    plt.show()

    print("Normed: ",trade1[normedKey])
    print("Predicted: ",predicedTrade[0][predictedKey])
    print("deNormed: ",deNormedTrade[0][predictedKey])
    print("upPreped: ",unPrepedTrade[predictedKey])
    print("original: ", originalTrade[originalKey])


if __name__ == '__main__':
    main()
    print("script end reached")








