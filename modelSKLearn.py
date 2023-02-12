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
matplotlib.use('Agg')
# matplotlib.use('TkCairo')
import matplotlib.pyplot as plt
import features as f
import pandas as pd
import dask.dataframe as dd
import logging
import argparse
import timing

targetTable = 'tradesCalculated_80ce611831f588a69bb698c37f5a8036'
scalerFileName = '20230212scaler.gz'

csvTimes = {
  'tenSeconds'       : '/mysqlFiles/tradesCalc_8036_medium_10s.csv',
  'thirtySeconds'    : '/mysqlFiles/tradesCalc_8036_medium_30s.csv',
  'ninetySeconds'    : '/mysqlFiles/tradesCalc_8036_medium_90s.csv',
  'fiveMinutes'      : '/mysqlFiles/tradesCalc_8036_medium_5m.csv',
  'fifteenMinutes'   : '/mysqlFiles/tradesCalc_8036_medium_15m.csv',
  'fortyFiveMinutes' : '/mysqlFiles/tradesCalc_8036_medium_45m.csv',
  'twoHours'         : '/mysqlFiles/tradesCalc_8036_medium_2h.csv',
}

csvNoNorm = '/mysqlFiles/tradesCalc_8036_medium_nonorm.csv'
csvNorm = '/mysqlFiles/tradesCalc_8036_medium_norm.csv'

selectQuery = f"""
SET @sql = CONCAT('SELECT ', (SELECT REPLACE(GROUP_CONCAT(COLUMN_NAME), 'index', '') FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{targetTable}' AND TABLE_SCHEMA = 'botscape'), ' FROM {targetTable}');
PREPARE stmt1 FROM @sql;
EXECUTE stmt1;
"""

def main():
  features = f.Features()
  Ycols = features.Ycols
  normalizer = dn.DataNormalizer(features, scalerFileName)
  del features
  # engine = mydb.getEngine()
  # df = pd.read_sql(selectQuery, con=engine)
  # engine.dispose()
  logging.info("begin loadin csv")
  dfNorm = dd.read_csv( csvNorm, assume_missing=True)
  logging.info("norm csv loaded")
  logging.info("normalizer init")
  dfNorm = normalizer.clipOutliersAllFeatures(dfNorm)
  logging.info("outliers clipped")
  dfNorm = normalizer.fitAndNormalizeAll(dfNorm)
  logging.info("normalization complete")

  dfNoNorm = dd.read_csv( csvNoNorm, assume_missing=True)
  dfNoNorm = dfNoNorm.compute()
  logging.info("no norm csv loaded")
  logging.info(f"noNorm {type(dfNoNorm)} shape {dfNoNorm.shape}")
  logging.info(f"norm {type(dfNorm)} shape {dfNorm.shape}")
  Xdf = pd.concat([dfNorm, dfNoNorm], axis=1)
  del dfNorm
  del dfNoNorm
  Xdf.columns = range(Xdf.shape[1])
  logging.info("Data combined and ready")
  # print(Xdf)

  for timeName, targetColumn in Ycols.items():
    logging.info(f"Split test traing {targetColumn}")
    Ydf = pd.read_csv( csvTimes[timeName] )
    Ydf = normalizer.clipOutliersAllFeatures(Ydf)
    logging.info("outliers clipped")
    Ydf = normalizer.fitAndNormalizeAll(Ydf)
      # , assume_missing=True)
    x_train, x_test, y_train, y_test = train_test_split(Xdf,Ydf, test_size = 0.05, random_state = 42)
    del Ydf
    logging.info(f"Starting Fit {targetColumn}")
    LR = LinearRegression()
    LR.fit(x_train,y_train)

    logging.info(f"Fit complete {targetColumn}")

    y_pred =  LR.predict(x_test)
    score=r2_score(y_test,y_pred)
    logging.info(f"Y {timeName} r2score is {score}")
    logging.info(f"mean_sqrd_error is== {mean_squared_error(y_test,y_pred)}")
    logging.info(f"root_mean_squared error of is== {np.sqrt(mean_squared_error(y_test,y_pred))}")

    plt.plot([-3, 3], [-3, 3], 'bo', linestyle="--")
    plt.scatter(y_test,y_pred)
    plt.grid(True)
    plt.xlabel('Actual')
    plt.ylabel('Predicted')
    plt.savefig(f'images/{timeName}_predictedVsActual', dpi=200)
    logging.info(f"Saved image for {targetColumn}")
    plt.close()


# def train(X, y, originalKey, predictedKey, normedKey, trade1):
#   x_train, x_test, y_train, y_test = train_test_split(X, y, test_size = 0.05, random_state = 42)
#   # creating an object of LinearRegression class
#   LR = LinearRegression()
#   # fitting the training data
#   LR.fit(x_train,y_train)

#   logging.info("Fit complete")

#   y_pred =  LR.predict(x_test)
#   score=r2_score(y_test,y_pred)
#   logging.info("Y5 score is ",score)
#   logging.info("mean_sqrd_error is==",mean_squared_error(y_test,y_pred))
#   logging.info("root_mean_squared error of is==",np.sqrt(mean_squared_error(y_test,y_pred)))
#   logging.info("trade1")
#   logging.info(trade1[0])
#   # logging.info(len(trade1))
#   result = db.selectAll(selectOriginalTrade % selectOriginalCols, (trade1[0],))
#   originalTrade = result[0]
#   # logging.info("result")
#   # logging.info(result)
#   # logging.info("OriginalTrade")
#   # logging.info(originalTrade)
#   # logging.info(len(originalTrade))
#   predicedValue = LR.predict([trade1[1:155]])
#   # logging.info("predicedValue")
#   # logging.info(predicedValue)
#   # logging.info(predicedValue.shape)
#   predicedTrade = np.zeros((1,162))
#   predicedTrade[0][predictedKey] = predicedValue
#   # logging.info("predicedTrade")
#   # logging.info(predicedTrade)
#   # logging.info(predicedTrade.shape)
#   deNormedTrade = dn.deNormTrade(predicedTrade, dn.scaler)
#   # logging.info("deNormedTrade")
#   # logging.info(deNormedTrade)
#   # logging.info(deNormedTrade.shape)
#   unPrepedTrade = dp.unprepY(deNormedTrade[0], originalTrade)
#   # logging.info("unPrepedTrade")
#   # logging.info(unPrepedTrade)
#   # logging.info(unPrepedTrade.shape)
#   # Make predictions using the testing set

#   plt.plot([-1, 1], [-1, 1], 'bo', linestyle="--")
#   plt.scatter(y_test,y_pred)
#   plt.grid(True)
#   plt.xlabel('Actual')
#   plt.ylabel('Predicted')
#   plt.show()

#   logging.info("Normed: ",trade1[normedKey])
#   logging.info("Predicted: ",predicedTrade[0][predictedKey])
#   logging.info("deNormed: ",deNormedTrade[0][predictedKey])
#   logging.info("upPreped: ",unPrepedTrade[predictedKey])
#   logging.info("original: ", originalTrade[originalKey])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument( '-log',
                         '--loglevel',
                         default='warning',
                         help='Provide logging level. Example --loglevel debug, default=warning' )

    args = parser.parse_args()

    logging.basicConfig( level=args.loglevel.upper() )
    logging.info( 'Logging now setup.' )
    timing.startTiming()
    main()
    logging.info("script end reached")








