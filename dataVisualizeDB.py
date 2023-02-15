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


csvNormalize = '/csvFiles/normalize.csv'
scalerFileName = '20130214scaler.gz'

def main():
  features = f.Features()
  df = pd.read_csv( csvNormalize ).astype('float32')

  for column in features.featuresToNormalize:
    logging.info(f'Making images/{column}_hist')
    plt.gcf().set_size_inches(15, 15)
    df[column].plot(kind='hist', bins=100)
    plt.savefig(f'images/{column}_hist', dpi=200)
    plt.close()

  normalizer = dn.DataNormalizer(features, scalerFileName)
  df = normalizer.clipOutliers(df, features.featuresToNormalize)

  for column in features.featuresToNormalize:
    logging.info(f'Making images/{column}_hist_afterOutlier')
    plt.gcf().set_size_inches(15, 15)
    df[column].plot(kind='hist', bins=100)
    plt.savefig(f'images/{column}_hist_afterOutlier', dpi=200)
    plt.close()

  df = normalizer.fitAndNormalize(df, features.featuresToNormalize)

  for column in features.featuresToNormalize:
    logging.info(f'Making images/{column}_hist_afterNorm')
    plt.gcf().set_size_inches(15, 15)
    df[column].plot(kind='hist', bins=100)
    plt.savefig(f'images/{date.today()}-{column}_hist_afterNorm', dpi=200)
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





