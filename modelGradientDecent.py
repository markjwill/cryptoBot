import numpy as np
import joblib
import os.path
import modelCore
from modelCore import logging
from modelCore import args
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import pandas as pd
from joblib import parallel_backend
import code

class ModelAlpha(modelCore.ModelCore):

  # From coursera course
  def computeCost(self, X, y, theta):
    m = len(y)
    J = 0
    s = np.power(( X.dot(theta) - np.transpose([y]) ), 2)
    J = (1.0/(2*m)) * s.sum( axis = 0 )
    return J

  # From coursera course
  def gradientDescentMulti(self, X, y, theta, alpha, num_iters, J_history, count, thetaFileName):
    m = len(y)
    for i in range(num_iters):
      theta = theta - alpha*(1.0/m) * np.transpose(X).dot(X.dot(theta) - np.transpose([y]))
      iterationNumber = i + count
      J_history[iterationNumber] = self.computeCost(X, y, theta)
      logging.info(f'J @ {iterationNumber} {J_history[iterationNumber]}')
    joblib.dump(theta, thetaFileName)
    return theta, J_history, count + num_iters

if __name__ == '__main__':
  logging.info('script start')
  model = ModelAlpha()
  model.initArgs(args)
  model.initFeatures()
  model.initNormalizer()

  model.downloadDataSet()
  model.dropOutliersInDataSet()
  model.normalizeDataSet()

  xdf = model.allData[model.xFeatures]

  for timeName, seconds in model.features.TIME_PERIODS.items():
    if model.singleTarget is not False and model.singleTarget != timeName:
      continue
    target = f'{timeName}_futurePrice'
    ydf = model.allData[target]

    thetaFileName = f'{model.modelDate}-theta-{timeName}.gz'

    if os.path.isfile(thetaFileName):
      theta = joblib.load(thetaFileName)
    else:
      theta = np.random.rand(len(model.xFeatures),1)

    alpha = 0.1
    num_iters = 3

    xTrainDf, xTestDf, yTrainDf, yTestDf = train_test_split(xdf,ydf, test_size = 0.25)

    xTrain = xTrainDf.to_numpy()
    yTrain = yTrainDf.to_numpy()

    count = 0
    J_history = {}
    theta, J_history, count = model.gradientDescentMulti(
        xTrain,
        yTrain,
        theta,
        alpha,
        num_iters,
        J_history,
        count,
        thetaFileName
      )

    # For interactive mode
    # theta, J_history, count = model.gradientDescentMulti( xTrain, yTrain, theta, alpha, num_iters, J_history, count, thetaFileName)

    # print("J history:", J_history[-50:])

    J = model.computeCost(xTrain, yTrain, theta)

    print("Test set cost: ", J)

    yPredictedDf = pd.DataFrame(np.array(xTestDf.to_numpy()).dot(theta), columns = [target] )
    with parallel_backend('threading', n_jobs=model.workers):
      score = r2_score(yTestDf,yPredictedDf)
      meanSquaredError = mean_squared_error(yTestDf,yPredictedDf)
      rootMeanSquared = np.sqrt(meanSquaredError)

    model.makeModelPerformanceImage(
      timeName,
      yTestDf,
      yPredictedDf,
      f'gradientDecent{model.isTest}',
      score,
      meanSquaredError,
      rootMeanSquared
    )
    # model.makeModelPerformanceImage(timeName,yTestDf,yPredictedDf,f'gradientDecent{model.isTest}',score,meanSquaredError,rootMeanSquared)

    print("script end reached")





