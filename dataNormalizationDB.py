# import dask.dataframe as dd
import pandas as pd
import mydb
import dataNormalization as dn
import features as f
import argparse
import timing
import logging


sourceTable = 'tradesCalculated_80ce611831f588a69bb698c37f5a8036'
destinationTable = 'tradesNormalized_80ce611831f588a69bb698c37f5a8036'
scalerFileName = '20130208scaler.gz'

def main():
  features = f.Features()
  engine = mydb.getEngine()
  df = pd.read_sql(f'SELECT * FROM {sourceTable};', con=engine)
  engine.dispose()
  features = f.Features()

  normalizer = dn.DataNormalizer(features, scalerFileName)
  df = normalizer.clipOutliers(df, features.featuresToNormalize)
  df = normalizer.fitAndNormalize(df, features.featuresToNormalize)

  engine = mydb.getEngine()
  df.to_sql(destinationTable, con = engine, if_exists = 'append')
  engine.dispose()


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






