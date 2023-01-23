from sklearn import preprocessing
import numpy as np

array = [-5.0, 0.0, 1.25, 2.5]
#Reshape your data using array.reshape(-1, 1) if your data has a single feature or array to make it 2D
array = np.array(array).reshape(-1, 1)
print("Array shape after reshaping it to a 2D array: ",array.shape)
scaler = preprocessing.MaxAbsScaler()
scaler = scaler.fit(array)
print("Scaled:", scaler.transform(np.array(array).reshape(-1, 1)))

print("Scaled2:", scaler.transform(np.array([-0.1, 0.1, 7.0, 10.0]).reshape(-1, 1)))
# Scale Test