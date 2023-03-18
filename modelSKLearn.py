import mydb as db
import numpy as np
import sys
from datetime import date
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
import memory_profiler
from joblib import parallel_backend


scalerFileName = '20230315scaler.gz'
dataDate = '2023-03-15'
featureDataFolder = '/home/debby/bot/csvFiles'
csvNoNormalize = f'{featureDataFolder}/{dataDate}-noNormalize.csv'
csvNormalize = f'{featureDataFolder}/{dataDate}-normalize.csv'

# @profile
def main():
  features = f.Features()
  normalizer = dn.DataNormalizer(features, scalerFileName)

  logging.info("begin loadin csv to normalize")
  dfNorm = pd.read_csv( csvNormalize ).astype('float32')
  logging.info("norm csv loaded")
  logging.info("normalizer init")
  dfNorm = normalizer.clipOutliersAllFeatures(dfNorm)
  logging.info("outliers clipped")
  with parallel_backend('threading', n_jobs=3):
    dfNorm = normalizer.fitAndNormalizeAll(dfNorm)
  logging.info("normalization complete")

  logging.info("begin loadin csv not needing to normalize")
  dfNoNorm = pd.read_csv( csvNoNormalize ).astype('float32')
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
    Ydf = pd.read_csv( f'{featureDataFolder}/{dataDate}-{timeName}.csv' )
    with parallel_backend('threading', n_jobs=3):
      # Ydf = normalizer.clipOutliersAllFeatures(Ydf)
      logging.info("outliers clipped")
      Ydf = normalizer.fitAndNormalizeAll(Ydf)
    x_train, x_test, y_train, y_test = train_test_split(Xdf,Ydf, test_size = 0.05, random_state = 42)
    del Ydf
    logging.info(f"Starting Fit {timeName} future price")
    with parallel_backend('threading', n_jobs=3):
      LR = LinearRegression()
      LR.fit(x_train,y_train)

    logging.info(f"Fit complete {timeName} future price")

    with parallel_backend('threading', n_jobs=3):
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
      f'-no-Y-clip -std-dev-2')
    plt.savefig(f'images/{date.today()}-{timeName}_predictedVsActual-noclip', dpi=200)
    logging.info(f"Saved image for {timeName} future price")
    plt.close()


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








