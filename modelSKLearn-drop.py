# import mydb as db
import numpy as np
import sys
from datetime import date
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_validate
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
import dataNormalization as dn
# import dataPrep as dp
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
import memory_profiler
from joblib import parallel_backend
import bucketConnector as bc


scalerFileName = '20230407scaler.gz'
dataDate = '2023-04-04'
workingFolder = '/home/admin/cryptoBot'
featureDataFolder = f'{workingFolder}/csvFiles'
imageFolder = f'{workingFolder}/images'
csvNoNormalize = f'{featureDataFolder}/{dataDate}-noNormalize.csv'
csvNormalize = f'{featureDataFolder}/{dataDate}-normalize.csv'
s3bucket = 'crypto-bot-bucket'
jobCount = 95

# @profile
def main():
  features = f.Features()
  normalizer = dn.DataNormalizer(features, scalerFileName)

  logging.info("begin loadin csv to normalize")
  # dfNorm = pd.read_csv( csvNormalize ).astype('float32')
  dfNorm = bc.downloadFile(csvNormalize, s3bucket)
  normCols = list(dfNorm.columns)
  dfNoNorm = bc.downloadFile(csvNoNormalize, s3bucket)
  Xcols = list(dfNorm.columns, dfNoNorm.columns)
  logging.info("norm csv loaded")
  logging.info("merge everything")
  allData = pd.concat([dfNorm, dfNoNorm], axis=1)

  for timeName, seconds in features.TIME_PERIODS.items():
    csvY = f'{featureDataFolder}/{dataDate}-{timeName}.csv'
    Ydf = bc.downloadFile(csvY, s3bucket)
    allData = pd.concat([allData, Ydf], axis=1)
    normCols.append(Ydf.columns)

  logging.info("drop outliers")
  allData = normalizer.dropOutliers(allData, normCols)
  logging.info("outliers dropped")
  logging.info("normalization")
  with parallel_backend('threading', n_jobs=jobCount):
    allData = normalizer.fitAndNormalizeAll(allData)
  logging.info("normalization complete")

  Xdf = allData[Xcols]

  Xdf.columns = range(Xdf.shape[1])
  logging.info("data dropped and normed")

  for timeName, seconds in features.TIME_PERIODS.items():

    logging.info(f"Split test traing {timeName} future price")

    column = f'{timeName}_futurePrice'
    Ydf = allData[column]
    filePath = f'{workingFolder}/{date.today()}-{column}_hist_drop_norm'
    logging.info(f'Making {filePath}')
    plt.gcf().set_size_inches(15, 15)
    Ydf[column].plot(kind='hist', bins=100)
    plt.savefig(filePath, dpi=200)
    plt.close()
    bc.uploadFile(f'{filePath}.png', s3bucket)

    x_train, x_test, y_train, y_test = train_test_split(Xdf,Ydf, test_size = 0.05, random_state = 42)
    del Ydf
    logging.info(f"Starting Fit {timeName} future price")
    with parallel_backend('threading', n_jobs=jobCount):
      LR = LinearRegression()
      LR.fit(x_train,y_train)

    logging.info(f"Fit complete {timeName} future price")

    with parallel_backend('threading', n_jobs=jobCount):
      y_pred = LR.predict(x_test)
      score = r2_score(y_test,y_pred)
      meanSquaredError = mean_squared_error(y_test,y_pred)
      rootMeanSquared = np.sqrt(meanSquaredError)
    logging.info(f"Y {timeName} r2score is {score:.5f}")
    logging.info(f"mean_sqrd_error is== {meanSquaredError:.5f}")
    logging.info(f"root_mean_squared error of is== {rootMeanSquared:.5f}")

    plt.plot([-1.5, 1.5], [-1.5, 1.5], 'bo', linestyle="--")
    plt.scatter(y_test,y_pred)
    plt.grid(True)
    plt.xlabel('Actual')
    plt.ylabel('Predicted')
    plt.subplots_adjust(top=0.7)
    plt.title(f'Y {timeName} r2score is {score:.5f}\n'
      f'mean_sqrd_error is {meanSquaredError:.5f}\n'
      f'root_mean_squared error of is {rootMeanSquared:.5f}\n'
      f'-dropped-outliers -std-dev-3')
    filePath = f'{imageFolder}/{date.today()}-{timeName}_predictedVsActual'
    plt.savefig(filePath, dpi=200)
    logging.info(f"Saved image for {timeName} future price")
    plt.close()
    bc.uploadFile(f'{filePath}.png', s3bucket)


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








