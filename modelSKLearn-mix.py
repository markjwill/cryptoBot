from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_validate
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import RidgeCV
from sklearn.linear_model import Ridge
from sklearn.neural_network import MLPRegressor
from sklearn.pipeline import Pipeline
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
from joblib import parallel_backend
import matplotlib.pyplot as plt
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import Pipeline
from datetime import date
import modelCore
from modelCore import logging
from modelCore import args
import numpy as np

if __name__ == '__main__':
  logging.info('script start')
  model = modelCore.ModelCore()
  model.initArgs(args)
  model.initFeatures()
  model.initNormalizer()

  model.downloadDataSet()
  model.dropOutliersInDataSet()
  model.normalizeDataSet()

  logging.info("Running model training")
  model.xdf = model.allData[model.xFeatures]
  model.xdf.columns = range(model.xdf.shape[1])
  if model.makeHistograms is not False:
    for column in model.xdf.columns:
      model.makeHistogramImage(model.xdf[column], column, f'hist_drop_norm-std2')

  timeName = 'tenSeconds'

  logging.info(f"Split test traing {timeName} future price")

  target = f'{timeName}_futurePrice'
  model.ydf = model.allData[target]
  if model.makeHistograms is not False:
    model.makeHistogramImage(model.ydf, target, f'hist_drop_norm-std2')

  model.xTrain, model.xTest, model.yTrain, model.yTest = train_test_split(model.xdf,model.ydf, test_size = 0.25)



  logging.info(f"Starting LinearRegression Fit {timeName} future price")
  with parallel_backend('threading', n_jobs=model.workers):
    LR = LinearRegression()
    LR.fit(model.xTrain,model.yTrain)

  logging.info(f"Fit complete {timeName} future price")

  with parallel_backend('threading', n_jobs=model.workers):
    yPredicted = LR.predict(model.xTest)
    score = r2_score(model.yTest,yPredicted)
    meanSquaredError = mean_squared_error(model.yTest,yPredicted)
    rootMeanSquared = np.sqrt(meanSquaredError)
  logging.info(f"Y {timeName} r2score is {score:.5f}")
  logging.info(f"mean_sqrd_error is== {meanSquaredError:.5f}")
  logging.info(f"root_mean_squared error of is== {rootMeanSquared:.5f}")

  model.makeModelPerformanceImage(
        timeName,
        model.yTest,
        yPredicted,
        f'skLearnDropLinearRegression-predictedVsActual-std2',
        score,
        meanSquaredError,
        rootMeanSquared
      )


  logging.info(f"Starting Ridge Fit {timeName} future price")
  with parallel_backend('threading', n_jobs=model.workers):
    RC = Ridge()
    RC.fit(model.xTrain,model.yTrain)

  logging.info(f"Fit complete {timeName} future price")

  with parallel_backend('threading', n_jobs=model.workers):
    yPredicted = RC.predict(model.xTest)
    score = r2_score(model.yTest,yPredicted)
    meanSquaredError = mean_squared_error(model.yTest,yPredicted)
    rootMeanSquared = np.sqrt(meanSquaredError)
  logging.info(f"Y {timeName} r2score is {score:.5f}")
  logging.info(f"mean_sqrd_error is== {meanSquaredError:.5f}")
  logging.info(f"root_mean_squared error of is== {rootMeanSquared:.5f}")

  model.makeModelPerformanceImage(
        timeName,
        model.yTest,
        yPredicted,
        f'skLearnDropRidge-predictedVsActual-std2',
        score,
        meanSquaredError,
        rootMeanSquared
      )


  logging.info(f"Starting RidgeCV Fit {timeName} future price")
  with parallel_backend('threading', n_jobs=model.workers):
    RCV = RidgeCV()
    RCV.fit(model.xTrain,model.yTrain)

  logging.info(f"Fit complete {timeName} future price")

  with parallel_backend('threading', n_jobs=model.workers):
    yPredicted = RCV.predict(model.xTest)
    score = r2_score(model.yTest,yPredicted)
    meanSquaredError = mean_squared_error(model.yTest,yPredicted)
    rootMeanSquared = np.sqrt(meanSquaredError)
  logging.info(f"Y {timeName} r2score is {score:.5f}")
  logging.info(f"mean_sqrd_error is== {meanSquaredError:.5f}")
  logging.info(f"root_mean_squared error of is== {rootMeanSquared:.5f}")

  model.makeModelPerformanceImage(
        timeName,
        model.yTest,
        yPredicted,
        f'skLearnDropRidge-predictedVsActual-std2',
        score,
        meanSquaredError,
        rootMeanSquared
      )


  logging.info(f"Starting NeuralNetwork Fit {timeName} future price")
  with parallel_backend('threading', n_jobs=model.workers):
    NN = MLPRegressor(hidden_layer_sizes=(400,))
    NN.fit(model.xTrain,model.yTrain)

  logging.info(f"Fit complete {timeName} future price")

  with parallel_backend('threading', n_jobs=model.workers):
    yPredicted = NN.predict(model.xTest)
    score = r2_score(model.yTest,yPredicted)
    meanSquaredError = mean_squared_error(model.yTest,yPredicted)
    rootMeanSquared = np.sqrt(meanSquaredError)
    logging.info(f"Y {timeName} r2score is {score:.5f}")
    logging.info(f"mean_sqrd_error is== {meanSquaredError:.5f}")
    logging.info(f"root_mean_squared error of is== {rootMeanSquared:.5f}")

    model.makeModelPerformanceImage(
          timeName,
          model.yTest,
          yPredicted,
          f'skLearnDropNeuralNetwork200-predictedVsActual-std2',
          score,
          meanSquaredError,
          rootMeanSquared
        )

  print("script end reached")





