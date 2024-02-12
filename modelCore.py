import os
import argparse
import logging
import sys
from datetime import date
from datetime import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import dataNormalization as dn
import bucketConnector as bc
from joblib import parallel_backend

parser = argparse.ArgumentParser()

parser.add_argument( '-log',
                     '--loglevel',
                     default='warning',
                     help='Provide logging level. Example --loglevel debug, default=warning' )

parser.add_argument( '-s3bucket',
                     '--s3bucket',
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

parser.add_argument( '-projectFolder',
                     '--projectFolder',
                     default='/home/admin/cryptoBot',
                     help='Provide temp local folder. The default is --projectFolder /home/admin/cryptoBot' )

parser.add_argument( '-dataDate',
                     '--dataDate',
                     default=date.today(),
                     help='Provide dateData was computed. Example --dataDate 2023-04-14. The default is todays date.' )

parser.add_argument( '-modelDate',
                     '--modelDate',
                     default=date.today(),
                     help='Provide date model was computed, or a new date to savea new model. Example --modelDate 2023-04-30. The default is todays date.' )

parser.add_argument( '-scalerDate',
                     '--scalerDate',
                     default=date.today(),
                     help='Provide scalerDate normalization scaler was computed, or a new date to rebuild scaler. Example --scalerDate 2023-04-14. The default is todays date.' )

parser.add_argument( '-singleTarget',
                     '--singleTarget',
                     default=None,
                     help='Provide a single Y target via timeName. Example --singleTarget fiveMinutes. The default is to run all Y targets.' )

parser.add_argument( '-makeHistograms',
                     '--makeHistograms',
                     default=False,
                     help='Make histogram images of each feature. Example --makeHistograms True. The default is False.' )

parser.add_argument( '-featuresDate',
                     '--featuresDate',
                     default='',
                     help='Use the feature file named with this date. Example --featuresFile 2023-06-09 will use features-223-06-09.py to determine what features to use. The default is an empty string which by convention, is the latest feature file.' )

args = parser.parse_args()

logging.basicConfig( level=args.loglevel.upper() )
logging.info( 'Logging now setup.' )

class ModelCore():

  isTest = ''
  scalerFileName = ''
  featureDataFolder = ''
  imageFolder = ''
  csvSource = ''
  logging = ''
  s3bucket = ''
  dataDate = ''
  modelDate = ''
  scalerDate = ''
  workers = 1
  makeHistograms = False
  singleTarget = ''
  featuresDate = ''

  featuresToNormalize = ''
  # featuresDontNormalize = ''

  allData = ''
  xFeatures = ''
  Ytargets = ''

  xTrain = ''
  xTest = ''
  yTrain = ''
  yTest = ''

  features = ''
  normalizer = ''

  xdf = []
  ydf = []

  def initArgs(self, args):
    self.isTest = ''
    if not args.useFullData:
      self.isTest = '-test'

    self.dataDate = args.dataDate
    self.modelDate = args.modelDate
    self.scalerDate = args.scalerDate

    self.workers = args.workers

    self.scalerFileName = f'{args.scalerDate}scaler.gz'
    self.featureDataFolder = f'{args.projectFolder}/csvFiles'
    self.imageFolder = f'{args.projectFolder}/images'
    self.csvSource = f'{self.featureDataFolder}/{self.dataDate}-all-columns{self.isTest}.csv'
    self.singleTarget = args.singleTarget
    self.makeHistograms = args.makeHistograms
    self.s3bucket = args.s3bucket
    self.featuresDate = args.featuresDate

  def initFeatures(self):
    featuresFile = 'features'
    if self.featuresDate !='':
      featuresFile = f'features-{self.featuresDate}'
    f = __import__(featuresFile)
    self.features = f.Features()

  def initNormalizer(self):
    if self.features == '':
      logging.error('initFeatures() must be run prior to initNormalizer, exiting.')
      sys.exit(1)
    if self.scalerFileName == '':
      logging.error('initArgs() must be run prior to initNormalizer, exiting.')
      sys.exit(1)
    self.normalizer = dn.DataNormalizer(self.features, self.scalerFileName)

  def downloadDataSet(self):
    logging.info("start loading data")
    self.allData = bc.downloadFile(self.csvSource, self.s3bucket)
    self.featuresToNormalize = self.features.featuresToNormalize
    self.xFeatures = [item for item in self.features.COLUMNS if 'future' not in item]


    
    # dfForNormalizing = bc.downloadFile(self.csvSource, self.s3bucket)
    # self.featuresToNormalize = list(dfForNormalizing)
    # dfDontNormalize = bc.downloadFile(self.csvNoNormalize, self.s3bucket)
    # self.xFeatures = list(dfForNormalizing.columns) + list(dfDontNormalize.columns)
    # logging.info("merge everything")
    # self.allData = pd.concat([dfForNormalizing, dfDontNormalize], axis=1)

    # for timeName, seconds in self.features.TIME_PERIODS.items():
    #   csvY = f'{self.featureDataFolder}/{self.dataDate}-{timeName}{self.isTest}.csv'
    #   Ydf = bc.downloadFile(csvY, self.s3bucket)
    #   self.allData = pd.concat([self.allData, Ydf], axis=1)
    #   self.featuresToNormalize = self.featuresToNormalize + Ydf.columns.tolist()
    # logging.info('data loading complete')

  def normalizeDataSet(self):
    logging.info("start normalization")
    with parallel_backend('threading', n_jobs=self.workers):
      self.allData = self.normalizer.fitAndNormalize(self.allData, self.featuresToNormalize)
    logging.info("normalization complete")

  def dropOutliersInDataSet(self):
    logging.info("start outlier drop")
    self.allData = self.normalizer.dropOutliers(self.allData, self.featuresToNormalize)
    logging.info("outlier drop complete")

  def clipOutliersInDataSet(self):
    logging.info("start outlier drop")
    self.allData = self.normalizer.clipOutliers(self.allData, self.featuresToNormalize)
    logging.info("outlier drop complete")

  def makeHistogramImage(self, df, column, detail):
    fileName = f'{datetime.today().strftime('%Y%m%d-%H%M%S')}-{column}{detail}{self.isTest}'
    filePath = f'{self.imageFolder}/../histograms/{fileName}'
    if not os.path.isfile(f'{filePath}.png'):
      logging.info(f'Making {filePath}')
      plt.gcf().set_size_inches(15, 15)
      df.plot(kind='hist', bins=100)
      plt.savefig(f'{filePath}.png', dpi=200)
      plt.close()
      # bc.uploadFile(f'{filePath}.png', self.s3bucket)

  def makeModelPerformanceImage(
      self,
      yName,
      yTest,
      yPredicted,
      detail,
      score,
      meanSquaredError,
      rootMeanSquared
    ):
    fileName = f'{datetime.today().strftime('%Y%m%d-%H%M%S')}-{yName}-predictedVsActual-{detail}{self.isTest}'
    filePath = f'{self.imageFolder }/{fileName}'
    logging.info(f'Making {filePath}')
    plt.plot([-1.1, 1.1], [-1.1, 1.1], 'bo', linestyle="--")
    plt.scatter(yTest,yPredicted, s=2)
    plt.grid(True)
    plt.xlabel('Actual')
    plt.ylabel('Predicted')
    plt.subplots_adjust(top=0.7)
    plt.title(f'Y {yName} r2score is {score:.5f}\n'
      f'mean_sqrd_error is {meanSquaredError:.5f}\n'
      f'root_mean_squared error of is {rootMeanSquared:.5f}\n'
      f'{detail}')
    plt.savefig(filePath, dpi=200)
    logging.info(f"Saved image for {yName} future price")
    plt.close()
    # bc.uploadFile(f'{filePath}.png', self.s3bucket)









