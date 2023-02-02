import sys
import mydb
import numpy as np
import dataNormalization as dn
from timeit import default_timer as timer
from datetime import timedelta
import timing
import pandas as pd
import features as f
import argparse
import logging

targetTable = 'tradesCalculated_old_clone'
scalerFileName = '20130202scaler.gz'

def main():
  engine = mydb.getEngine()
  df = pd.read_sql(f'SELECT * FROM {targetTable};', con=engine)
  engine.dispose()
  features = f.Features()
  normalizer = dn.DataNormalizer(features, scalerFileName)

  df.plot.scatter(x='volume',
                  y='thirtySeconds_futurePrice',
                  colormap='viridis')




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





