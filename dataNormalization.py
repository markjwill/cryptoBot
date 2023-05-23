from sklearn import preprocessing
import joblib
import os.path
import dask.dataframe as dd
from scipy import stats
import logging
import pandas as pd
import numpy as np

class DataNormalizer:

  outlierStandardDeviations = 2
  scalerFileName = ''
  scaler = None

  def __init__(self, features, scalerFileName):
    self.scalerFileName = scalerFileName
    if os.path.isfile(self.scalerFileName):
      self.scaler = joblib.load(self.scalerFileName)
    else:
      self.initScaler()

  def initScaler(self):
    self.scaler = preprocessing.MaxAbsScaler()
    self.saveScaler()

  def saveScaler(self):
    joblib.dump(self.scaler, self.scalerFileName)

  def clipOutliersAllFeatures(self, df):
    means = df.mean(axis=0)
    standardDeviations = df.std(axis=0)
    uppers = means + ( standardDeviations * self.outlierStandardDeviations )
    lowers = means - ( standardDeviations * self.outlierStandardDeviations )
    if isinstance(df, dd.core.DataFrame):
      df = df.compute()
    logging.debug(type(df))
    df = df.clip(lower=lowers, upper=uppers, axis=1)
    return df

  def clipOutliers(self, df, featuresToNormalize):
    means = df[featuresToNormalize].mean(axis=0)
    standardDeviations = df[featuresToNormalize].std(axis=0)
    uppers = means + ( standardDeviations * self.outlierStandardDeviations )
    lowers = means - ( standardDeviations * self.outlierStandardDeviations )
    df[featuresToNormalize] = df[featuresToNormalize].clip(lower=lowers, upper=uppers, axis=1)
    return df

  def dropOutliers(self, df, featuresToCheck):
    featuresToCheck = [str(f) for f in featuresToCheck]
    dfFeatures = [str(f) for f in list(df.columns)]
    if not set(featuresToCheck).issubset(set(dfFeatures)):
      logging.info('featuresToCheck')
      logging.info(featuresToCheck)
      logging.info('dfFeatures')
      logging.info(dfFeatures)
      raise ValueError("One or more columns in featuresToCheck not found in dataframe")
    dfNorm = df.loc[:, featuresToCheck]
    outliers = np.abs((dfNorm - dfNorm.mean()) / dfNorm.std(ddof=0)) < self.outlierStandardDeviations
    df.loc[:, featuresToCheck] = dfNorm.where(outliers, np.nan)
    df.dropna(inplace=True)
    return df

  def batchScalerBuild(self, df):
    self.scaler = self.scaler.partial_fit(df)
    self.saveScaler()

  def fitAndNormalizeAll(self, df):
    columns = df.columns
    df = self.scaler.fit_transform(df)
    self.saveScaler()
    return pd.DataFrame(df, columns=columns)

  def fitAndNormalize(self, df, featuresToNormalize):
    df[featuresToNormalize] = self.scaler.fit_transform(df[featuresToNormalize])
    self.saveScaler()
    return df

  def normalize(self, df, featuresToNormalize):
    df[featuresToNormalize] = self.scaler.transform(df[featuresToNormalize])
    return df

  def deNormalize(self, trade, featuresToNormalize):
    df[featuresToNormalize] = self.scaler.inverse_transform(df[featuresToNormalize])
    return df









