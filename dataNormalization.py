from sklearn import preprocessing
import joblib
import os.path
import dask.array as da
from scipy import stats

class DataNormalizer:
  
  scalerFileName = ''
  scaler = None

  def __init__(self, features, scalerFileName):
    self.scalerFileName = scalerFileName
    if os.path.isfile(self.scalerFileName):
      self.scaler = joblib.load(self.scalerFileName)
    else:
      self.scaler = self.initScaler()

  def initScaler(self):
    scaler = preprocessing.MaxAbsScaler()
    self.saveScaler()
    return scaler

  def saveScaler(self):
    joblib.dump(self.scaler, self.scalerFileName)

  def removeOutliers(self, df, featuresToNormalize):
    means = df[featuresToNormalize].mean(axis=0)
    standardDeviations = df[featuresToNormalize].std(axis=0)
    uppers = means + ( standardDeviations * 3 )
    lowers = means - ( standardDeviations * 3 )
    df[featuresToNormalize] = df.clip(lower=lowers, upper=uppers, axis=0)
    # df[(da.abs(stats.zscore(df[featuresToNormalize])) < 3).all(axis=1)]
    return df

  def batchScalerBuild(self, df):
    self.scaler = self.scaler.partial_fit(df)
    self.saveScaler()

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









