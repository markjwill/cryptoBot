from sklearn import preprocessing
import joblib
import os.path

scalerFileName = 'dataScalerV20221216.gz'

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





