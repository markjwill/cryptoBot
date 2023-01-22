from sklearn import preprocessing
import joblib
import os.path
import features

scalerFileName = 'dataScalerV20221216.gz'

def removeOutliers(df, features):
  normDf = df[i for i in df if i not in features.DO_NOT_NORMALIZE]
  df[(np.abs(stats.zscore(normDf)) < 3).all(axis=1)]
  return df

def batchScalerBuild(trades, scaler, scalerFileName):
  scaler = scaler.partial_fit(trades)
  joblib.dump(scaler, scalerFileName)

def normTrades(trades, scaler):
  return scaler.transform(trades)

def normTrade(trade, scaler):
  return scaler.transform([trade])

def deNormTrade(trade, scaler):
  return scaler.inverse_transform(trade)

def initScaler(scalerFileName):
  scaler = preprocessing.MaxAbsScaler()
  joblib.dump(scaler, scalerFileName)
  return scaler

if os.path.isfile(scalerFileName):
  scaler = joblib.load(scalerFileName)
else:
  scaler = initScaler(scalerFileName)





