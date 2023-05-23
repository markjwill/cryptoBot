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
  logging.info("norm csv loaded")
  logging.info("normalizer init")
  dfNorm = normalizer.clipOutliersAllFeatures(dfNorm)
  logging.info("outliers clipped")
  with parallel_backend('threading', n_jobs=jobCount):
    dfNorm = normalizer.fitAndNormalizeAll(dfNorm)
  logging.info("normalization complete")

  logging.info("begin loadin csv not needing to normalize")
  # dfNoNorm = pd.read_csv( csvNoNormalize ).astype('float32')
  dfNoNorm = bc.downloadFile(csvNoNormalize, s3bucket)
  logging.info("no normalize csv loaded")
  logging.debug(f"noNorm {type(dfNoNorm)} shape {dfNoNorm.shape}")
  logging.debug(f"norm {type(dfNorm)} shape {dfNorm.shape}")
  Xdf = pd.concat([dfNorm, dfNoNorm], axis=1)
  del dfNorm
  del dfNoNorm
  Xdf.columns = range(Xdf.shape[1])
  logging.info("data combined and ready")

  for timeName, seconds in features.TIME_PERIODS.items():

    logging.info(f"Split test traing {timeName} future price")
    csvY = f'{featureDataFolder}/{dataDate}-{timeName}.csv'
    # Ydf = pd.read_csv(csvY)
    Ydf = bc.downloadFile(csvY, s3bucket)

    column = f'{timeName}_futurePrice'
    logging.info(f'Making images/{date.today()}-{column}_hist')
    plt.gcf().set_size_inches(15, 15)
    Ydf[column].plot(kind='hist', bins=100)
    filePath = f'{workingFolder}/{date.today()}-{column}_hist'
    plt.savefig(filePath, dpi=200)
    plt.close()
    bc.uploadFile(f'{filePath}.png', s3bucket)

    with parallel_backend('threading', n_jobs=jobCount):
      Ydf = normalizer.clipOutliersAllFeatures(Ydf)
      logging.info("outliers clipped")

      logging.info(f'Making images/{date.today()}-{column}_hist')
      plt.gcf().set_size_inches(15, 15)
      Ydf[column].plot(kind='hist', bins=100)
      filePath = f'{workingFolder}/{date.today()}-{column}_hist'
      plt.savefig(filePath, dpi=200)
      plt.close()
      bc.uploadFile(f'{filePath}.png', s3bucket)

      Ydf = normalizer.fitAndNormalizeAll(Ydf)
    x_train, x_test, y_train, y_test = train_test_split(Xdf,Ydf, test_size = 0.25)
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
      f'-Y-clip -std-dev-2')
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








