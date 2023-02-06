import sys
import mydb
import numpy as np
import dataNormalization as dn
from timeit import default_timer as timer
from datetime import timedelta
# import dask.dataframe as dd
import timing
import pandas as pd
import features as f
import argparse
import logging
import matplotlib
matplotlib.use('TkCairo')
import matplotlib.pyplot as plt


targetTable = 'tradesCalculated_80ce611831f588a69bb698c37f5a8036'
scalerFileName = '20130202scaler.gz'

def main():
  features = f.Features()
  logging.info("Features to normalize")
  logging.info(features.featuresToNormalize)
  engine = mydb.getEngine()
  # f'SELECT * FROM {targetTable} AS t1 JOIN (SELECT index FROM {targetTable} ORDER BY RAND() LIMIT 5000) as t2 ON t1.index=t2.index;'
  df = pd.read_sql(f'SELECT*FROM {targetTable} AS t1 JOIN(SELECT {targetTable}.index FROM {targetTable} ORDER BY RAND()LIMIT 100)AS t2 ON t1.index=t2.index;', con=engine)
  # df = pd.read_sql(f'SELECT * FROM {targetTable} ORDER BY RAND() LIMIT 10000;', con=engine)
  # df = pd.read_sql(f'SELECT * FROM {targetTable} LIMIT 10000;', con=engine)
  engine.dispose()
  features = f.Features()
  # for column in features.COLUMNS:
  #   logging.info(f'Making images/{column}_hist')
  #   plt.gcf().set_size_inches(15, 15)
  #   df[column].plot(kind='hist', bins=100)
  #   plt.savefig(f'images/{column}_hist', dpi=200)
  #   plt.close()
    # df[column].plot(kind='kde')
    # plt.savefig(f'images/{column}_hist')

  
  normalizer = dn.DataNormalizer(features, scalerFileName)
  df = normalizer.removeOutliers(df, features.featuresToNormalize)
  df = normalizer.fitAndNormalize(df, features.featuresToNormalize)



  for column in features.featuresToNormalize:
    logging.info(f'Making images/{column}_hist_afterNorm')
    plt.gcf().set_size_inches(15, 15)
    df[column].plot(kind='hist', bins=100)
    plt.savefig(f'images/{column}_hist_afterNorm', dpi=200)
    plt.close()
    # df[column].plot(kind='kde')
    # plt.savefig(f'images/{column}_hist')

  # plt.show()
  # df.plot.scatter(x='twoHours_exchange_buyVsSellVolume',
  #                 y='twoHours_futurePrice')
  # plt.show()
  # # plt.savefig('priceXbsV')
  # df.plot.scatter(x='fiveMinutes_exchange_lowPrice',
  #                 y='fiveMinutes_exchange_highPrice')
  # plt.show()
  # # plt.savefig('highVlow')
  # df.plot.scatter(x='fiveMinutes_tradeCount',
  #                 y='fiveMinutes_exchange_upVsDown')
  # plt.show()
  # # plt.savefig('countVud')
  # df.plot.scatter(x='fiveMinutes_exchange_changeReal',
  #                 y='fiveMinutes_exchange_travelReal')
  # plt.show()
  # # plt.savefig('changeVtravel')

  # features = f.Features()
  # normalizer = dn.DataNormalizer(features, scalerFileName)



# def buildScaler():
#   start = timer()
#   i = 0
#   while True:
#   # while True:
#     offset = (i * 1000)
#     print("build scaler OFFSET: %s"% offset)
#     i += 1
#     results = mydb.selectAll(selectForBuild % selectCols, (offset,))

#     if len(results) == 0:
#       break
#     # print("Results:", results[0])
#     array = np.array(results)
#     # print("Data: ", array)
#     dn.batchScalerBuild(array, dn.scaler, dn.scalerFileName)
#     end = timer()
#     print("elapsed: ",timedelta(seconds=end-start))

# def normalizeAndSave():
#   i = 0
#   while True:
#     start = timer()
#     offset = (i * 1000)
#     i += 1
#     results = mydb.selectAll(selectForNorm % selectCols, (offset,))

#     if len(results) == 0:
#       break
#     array = np.array(results)
#     normed = dn.normTrades(array, dn.scaler)
#     insertData = list(normed)

#     for a in range(len(insertData)):
#       insertData[a] = list(insertData[a])
#       insertData[a][0] = results[a][0]
#       insertData[a] = tuple(insertData[a])

#     end = timer()
#     print("elapsed: ",timedelta(seconds=end-start))
#     mydb.insertMany(insertNormed % insertCols, insertData )
#     print("finished OFFSET: %s"% offset)
#     end = timer()



if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # parser.add_argument( '-b',
    #                      '--build',
    #                      default=0,
    #                      const=0,
    #                      nargs='?',
    #                      help='If present the scaler will be built' )

    # parser.add_argument( '-n',
    #                      '--norm',
    #                      default=0,
    #                      const=0,
    #                      nargs='?',
    #                      help='If present the scaler will be applied to the data normalizing it.' )

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

# if len(sys.argv) == 1:
#   scriptError()
# if sys.argv[1] == "-build":
#   buildScaler()
# elif  sys.argv[1] == "-norm":
#   normalizeAndSave()
# else:
#   scriptError()

# print("script end reached")





