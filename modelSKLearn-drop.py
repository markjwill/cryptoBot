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

# @profile
def main(
    s3bucket,
    useFullData,
    workers,
    workingFolder,
    dataDate,
    scalerDate
  ):

  isTest = ''
  if not useFullData:
    isTest = '-test'

  scalerFileName = f'{scalerDate}scaler.gz'
  featureDataFolder = f'{workingFolder}/csvFiles'
  imageFolder = f'{workingFolder}/images'
  csvNoNormalize = f'{featureDataFolder}/{dataDate}-noNormalize{isTest}.csv'
  csvNormalize = f'{featureDataFolder}/{dataDate}-normalize{isTest}.csv'

  features = f.Features()
  normalizer = dn.DataNormalizer(features, scalerFileName)

  logging.info("begin loadin csv to normalize")
  # dfNorm = pd.read_csv( csvNormalize ).astype('float32')
  dfNorm = bc.downloadFile(csvNormalize, s3bucket)
  normCols = list(dfNorm)
  dfNoNorm = bc.downloadFile(csvNoNormalize, s3bucket)
  Xcols = list(dfNorm.columns) + list(dfNoNorm.columns)
  logging.info("norm csv loaded")
  logging.info("merge everything")
  allData = pd.concat([dfNorm, dfNoNorm], axis=1)

  for timeName, seconds in features.TIME_PERIODS.items():
    csvY = f'{featureDataFolder}/{dataDate}-{timeName}{isTest}.csv'
    Ydf = bc.downloadFile(csvY, s3bucket)
    allData = pd.concat([allData, Ydf], axis=1)
    normCols = normCols + Ydf.columns.tolist()

  logging.info(f"drop outliers pre-row count: {allData.shape[0]} X {allData.shape[1]}")
  allData = normalizer.dropOutliers(allData, normCols)
  logging.info(f"outliers dropped new row count: {allData.shape[0]} X {allData.shape[1]}")
  logging.info("normalization")
  with parallel_backend('threading', n_jobs=workers):
    allData = normalizer.fitAndNormalize(allData, normCols)
  logging.info("normalization complete")

  logging.info(f'allData - Xcols check')
  logging.info(list(set(allData.columns.tolist()) - set(Xcols)))
  logging.info(f'Xcols - allData check')
  logging.info(list(set(Xcols) - set(allData.columns.tolist())))

  Xdf = allData[Xcols]

  Xdf.columns = range(Xdf.shape[1])
  logging.info("data dropped and normed")

  for timeName, seconds in features.TIME_PERIODS.items():

    logging.info(f"Split test traing {timeName} future price")

    column = f'{timeName}_futurePrice'
    Ydf = allData[column]
    filePath = f'{workingFolder}/{date.today()}-{column}_hist_drop_norm-std2-test'
    logging.info(f'Making {filePath}')
    plt.gcf().set_size_inches(15, 15)
    Ydf.plot(kind='hist', bins=100)
    plt.savefig(filePath, dpi=200)
    plt.close()
    bc.uploadFile(f'{filePath}.png', s3bucket)

    x_train, x_test, y_train, y_test = train_test_split(Xdf,Ydf, test_size = 0.25)
    del Ydf
    logging.info(f"Starting Fit {timeName} future price")
    with parallel_backend('threading', n_jobs=workers):
      LR = LinearRegression(copy_X=True)
      LR.fit(x_train,y_train)

    logging.info(f"Fit complete {timeName} future price")

    with parallel_backend('threading', n_jobs=workers):
      y_pred = LR.predict(x_test)
      score = r2_score(y_test,y_pred)
      meanSquaredError = mean_squared_error(y_test,y_pred)
      rootMeanSquared = np.sqrt(meanSquaredError)
    logging.info(f"Y {timeName} r2score is {score:.5f}")
    logging.info(f"mean_sqrd_error is== {meanSquaredError:.5f}")
    logging.info(f"root_mean_squared error of is== {rootMeanSquared:.5f}")

    plt.plot([-1.5, 1.5], [-1.5, 1.5], 'bo', linestyle="--")
    plt.scatter(y_test,y_pred, s=2)
    plt.grid(True)
    plt.xlabel('Actual')
    plt.ylabel('Predicted')
    plt.subplots_adjust(top=0.7)
    plt.title(f'Y {timeName} r2score is {score:.5f}\n'
      f'mean_sqrd_error is {meanSquaredError:.5f}\n'
      f'root_mean_squared error of is {rootMeanSquared:.5f}\n'
      f'-dropped-outliers -std-dev-2')
    filePath = f'{imageFolder}/{date.today()}-{timeName}_predictedVsActual-std2-test'
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

  parser.add_argument( '-bucket',
                       '--bucket',
                       default='crypto-bot-bucket',
                       help='Provide the name of the s3 bucket. Example -bucket crypto-bot-bucket, default=crypto-bot-bucket' )

  parser.add_argument( '-useFullData',
                       '--useFullData',
                       default=False,
                       action=argparse.BooleanOptionalAction,
                       help='Provide instruction to use full data or test data. Example --useFullData, default will use code testing dataset' )

  parser.add_argument( '-workers',
                       '--workers',
                       default=12,
                       help='Provide number of worker processes. ~1 pr cpu is reccomended. Example --workers 48, default 12' )

  parser.add_argument( '-folder',
                       '--folder',
                       help='Provide temp local folder. Example --folder /home/admin/cryptoBot/csvFiles, required, there is no default' )

  parser.add_argument( '-dataDate',
                       '--dataDate',
                       help='Provide dateData was computed. Example --dataDate 2023-04-14, required, there is no default' )

  parser.add_argument( '-scalerDate',
                       '--scalerDate',
                       help='Provide scalerDate normalization scaler was computed, or a new date to rebuild scaler. Example --scalerDate 20230414, there is no default' )

  args = parser.parse_args()

  logging.basicConfig( level=args.loglevel.upper() )
  logging.info( 'Logging now setup.' )
  timing.startTiming()
  main(
    args.bucket,
    args.useFullData,
    args.workers,
    args.folder,
    args.dataDate,
    args.scalerDate
  )
  logging.info("script end reached")








