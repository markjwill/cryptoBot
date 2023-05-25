import argparse
import logging
import sys
from datetime import date
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import features as f
import dataNormalization as dn
import bucketConnector as bc

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
                     default='/home/admin/cryptoBot'
                     help='Provide temp local folder. The default is --folder /home/admin/cryptoBot' )

parser.add_argument( '-dataDate',
                     '--dataDate',
                     default=date.today()
                     help='Provide dateData was computed. Example --dataDate 2023-04-14. The default is todays date.' )

parser.add_argument( '-scalerDate',
                     '--scalerDate',
                     default=date.today()
                     help='Provide scalerDate normalization scaler was computed, or a new date to rebuild scaler. Example --scalerDate 2023-04-14. The default is todays date.' )

args = parser.parse_args()

logging.basicConfig( level=args.loglevel.upper() )
logging.info( 'Logging now setup.' )

class ModelCore():

  isTest = ''
  scalerFileName = ''
  featureDataFolder = ''
  imageFolder = ''
  csvNoNormalize = ''
  csvNormalize = ''
  logging = ''
  s3bucket = ''

  features = ''
  normalizer = ''

  def initArgs(args):
    isTest = ''
    if not args.useFullData:
      self.isTest = '-test'

    self.scalerFileName = f'{args.scalerDate}scaler.gz'
    self.featureDataFolder = f'{args.projectFolder}/csvFiles'
    self.imageFolder = f'{args.projectFolder}/images'
    self.csvNoNormalize = f'{args.featureDataFolder}/{args.dataDate}-noNormalize{isTest}.csv'
    self.csvNormalize = f'{args.featureDataFolder}/{args.dataDate}-normalize{isTest}.csv'
    self.s3bucket = args.s3bucket

  def initFeatures():
    self.features = f.Features()

  def initNormalizer():
    if self.features == '':
      logging.error('initFeatures() must be run prior to initNormalizer, exiting.')
      sys.exit(1)
    if self.scalerFileName == '':
      logging.error('initArgs() must be run prior to initNormalizer, exiting.')
      sys.exit(1)
    self.normalizer = dn.DataNormalizer(self.features, self.scalerFileName)

  def makeHistogramImage(df, column, detail):
    filePath = f'{self.workingFolder}/{date.today()}-{column}{detail}{self.isTest}'
    if not os.path.isfile(f'{filePath}.png'):
      logging.info(f'Making {filePath}')
      plt.gcf().set_size_inches(15, 15)
      df[column].plot(kind='hist', bins=100)
      plt.savefig(filePath, dpi=200)
      plt.close()
      bc.uploadFile(f'{filePath}.png', self.s3bucket)

  def makeModelPerformanceImage(
      yName,
      yTest,
      yPredicted,
      score,
      meanSquaredError,
      rootMeanSquared
    ):
    filePath = f'{self.workingFolder}/{date.today()}-{yName}-predictedVsActual-{detail}{self.isTest}'
    logging.info(f'Making {filePath}')
    plt.plot([-1.5, 1.5], [-1.5, 1.5], 'bo', linestyle="--")
    plt.scatter(yTest,yPredicted, s=2)
    plt.grid(True)
    plt.xlabel('Actual')
    plt.ylabel('Predicted')
    plt.subplots_adjust(top=0.7)
    plt.title(f'Y {yName} r2score is {score:.5f}\n'
      f'mean_sqrd_error is {meanSquaredError:.5f}\n'
      f'root_mean_squared error of is {rootMeanSquared:.5f}\n'
      f'-dropped-outliers -std-dev-2')
    plt.savefig(filePath, dpi=200)
    logging.info(f"Saved image for {yName} future price")
    plt.close()
    bc.uploadFile(f'{filePath}.png', self.s3bucket)









