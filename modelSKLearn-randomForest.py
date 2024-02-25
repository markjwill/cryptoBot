from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_validate
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
from joblib import parallel_backend
import modelCore
from modelCore import logging
from modelCore import args
import numpy as np

class ModelSKLearnClip(modelCore.ModelCore):

  def run(self):

    xdf = self.allData[self.xFeatures]
    xdf.columns = range(xdf.shape[1])


    for timeName, seconds in self.features.TIME_PERIODS.items():
      if self.singleTarget is not False and self.singleTarget != timeName:
        continue
      logging.info(f"Split test traing {timeName} future price")

      target = f'{timeName}_futurePrice'
      ydf = self.allData[target]
      if self.makeHistograms is not False:
        self.makeHistogramImage(ydf, target, f'hist_drop_norm-std2{self.isTest}')

      xTrain, xTest, yTrain, yTest = train_test_split(xdf,ydf, test_size = 0.25)
      del ydf
      logging.info(f"Starting Fit {timeName} future price")
      with parallel_backend('threading', n_jobs=self.workers):
        RFR = RandomForestRegressor(copy_X=True)
        RFR.fit(xTrain,yTrain)

      logging.info(f"Fit complete {timeName} future price")

      with parallel_backend('threading', n_jobs=self.workers):
        yPredicted = RFR.predict(xTest)
        score = r2_score(yTest,yPredicted)
        meanSquaredError = mean_squared_error(yTest,yPredicted)
        rootMeanSquared = np.sqrt(meanSquaredError)
      logging.info(f"Y {timeName} r2score is {score:.5f}")
      logging.info(f"mean_sqrd_error is== {meanSquaredError:.5f}")
      logging.info(f"root_mean_squared error of is== {rootMeanSquared:.5f}")

      self.makeModelPerformanceImage(
            timeName,
            yTest,
            yPredicted,
            f'skLearn-RFR-predictedVsActual-std2{self.isTest}',
            score,
            meanSquaredError,
            rootMeanSquared
          )

if __name__ == '__main__':
  logging.info('script start')
  model = ModelSKLearnClip()
  model.initArgs(args)
  model.initFeatures()
  model.initNormalizer()

  model.downloadDataSet()
  model.dropOutliersInDataSet()
  model.normalizeDataSet()

  model.run()
  print("script end reached")





