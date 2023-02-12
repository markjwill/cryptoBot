# import dask.dataframe as dd
import pandas as pd
import mydb
import dataNormalization as dn
import features as f
import argparse
import timing
import logging
import matplotlib
matplotlib.use('TkCairo')
import matplotlib.pyplot as plt


targetTable = 'tradesCalculated_80ce611831f588a69bb698c37f5a8036'
scalerFileName = '20130202scaler.gz'

def main():
  features = f.Features()
  engine = mydb.getEngine()
  df = pd.read_sql(f'SELECT * FROM {targetTable};', con=engine)
  engine.dispose()
  features = f.Features()

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
    plt.savefig(f'images/{column}_hist_afterNorm', dpi=200)
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





