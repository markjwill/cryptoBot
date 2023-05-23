# import dask.dataframe as dd
import pandas as pd
import dataNormalization as dn
import features as f
import argparse
import timing
import logging
import matplotlib
matplotlib.use('Agg')
# matplotlib.use('TkCairo')
import matplotlib.pyplot as plt
from datetime import date
import bucketConnector as bc


csvNormalize = '/home/admin/cryptoBot/csvFiles/2023-04-04-normalize.csv'
scalerFileName = '20130406scaler.gz'
workingFolder = '/home/admin/cryptoBot/images'
s3bucket = 'crypto-bot-bucket'

def main():
  features = f.Features()
  df = pd.read_csv( csvNormalize ).astype('float32')

  # for column in features.featuresToNormalize:
  #   logging.info(f'Making images/{column}_hist')
  #   plt.gcf().set_size_inches(15, 15)
  #   df[column].plot(kind='hist', bins=100)
  #   filePath = f'{workingFolder}/{date.today()}-{column}_hist'
  #   plt.savefig(filePath, dpi=200)
  #   plt.close()
  #   bc.uploadFile(f'{filePath}.png', s3bucket)

  normalizer = dn.DataNormalizer(features, scalerFileName)
  df = normalizer.clipOutliers(df, features.featuresToNormalize)

  for column in features.featuresToNormalize:
    logging.info(f'Making images/{date.today()}-{column}_hist_afterOutlier')
    plt.gcf().set_size_inches(15, 15)
    df[column].plot(kind='hist', bins=100)
    filePath = f'{workingFolder}/{date.today()}-{column}_hist_afterOutlier'
    plt.savefig(filePath, dpi=200)
    plt.close()
    bc.uploadFile(f'{filePath}.png', s3bucket)

  df = normalizer.fitAndNormalize(df, features.featuresToNormalize)

  for column in features.featuresToNormalize:
    logging.info(f'Making images/{column}_hist_afterNorm')
    plt.gcf().set_size_inches(15, 15)
    df[column].plot(kind='hist', bins=100)
    filePath = f'{workingFolder}/{date.today()}-{column}_hist_afterNorm'
    plt.savefig(filePath, dpi=200)
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





