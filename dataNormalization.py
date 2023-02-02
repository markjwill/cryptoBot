from sklearn import preprocessing
import joblib
import os.path

class DataNormalizer:
  
  scalerFileName = ''
  scaler = None
  featuresToNormalize = []

  def __init__(self, features, scalerFileName):
    self.scalerFileName = scalerFileName
    if os.path.isfile(self.scalerFileName):
      self.scaler = joblib.load(self.scalerFileName)
    else:
      self.scaler = self.initScaler()
    self.featuresToNormalize = [i for i in features.COLUMNS if i not in features.DO_NOT_NORMALIZE]

  def initScaler(self):
    scaler = preprocessing.MaxAbsScaler()
    self.saveScaler()
    return scaler

  def saveScaler(self):
    joblib.dump(self.scaler, self.scalerFileName)

  def removeOutliers(self, df):
    df[(np.abs(stats.zscore(self.featuresToNormalize)) < 3).all(axis=1)]
    return df

  def batchScalerBuild(self, df):
    self.scaler = self.scaler.partial_fit(df)
    self.saveScaler()

  def fitAndNormalize(self, df):
    df[self.featuresToNormalize] = self.scaler.fit_transform(df[self.featuresToNormalize])
    self.saveScaler()
    return df

  def normalize(self, df):
    df[self.featuresToNormalize] = self.scaler.transform(df[self.featuresToNormalize])
    return df

  def deNormalize(self, trade):
    df[self.featuresToNormalize] = self.scaler.inverse_transform(df[self.featuresToNormalize])
    return df









