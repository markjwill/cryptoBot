#!/usr/bin/python

import mariadb
import credentials
import numpy as np
import sys

try:
  self.conn = mariadb.connect(
    user=credentials.dbUser,
    password=credentials.dbPassword,
    host=credentials.dbHost,
    port=credentials.dbPort,
    database=credentials.dbName
  )
except mariadb.Error as e:
  print(f"Error connecting to MariaDB Platform: {e}")
  sys.exit(1)

# Get Cursor
self.cur = self.conn.cursor()

import numpy as np

def computeCost(X, y, theta):


    #COMPUTECOST Compute cost for linear regression
    #   J = COMPUTECOST(X, y, theta) computes the cost of using theta as the
    #   parameter for linear regression to fit the data points in X and y

    # Initialize some useful values
    m = len(y) # number of training examples

    # You need to return the following variables correctly 
    J = 0

    # ====================== YOUR CODE HERE ======================
    # Instructions: Compute the cost of a particular choice of theta
    #               You should set J to the cost.

	# note that 
	#	theta is an (n+1)-dimensional vector 
	#	X is an m x (n+1)-dimensional matrix
	#	y is an m-dimensional vector

    s = np.power(( X.dot(theta) - np.transpose([y]) ), 2)
    J = (1.0/(2*m)) * s.sum( axis = 0 )

    return J

def gradientDescentMulti(X, y, theta, alpha, num_iters):

    #GRADIENTDESCENTMULTI Performs gradient descent to learn theta
    #   theta = GRADIENTDESCENTMULTI(x, y, theta, alpha, num_iters) updates theta by
    #   taking num_iters gradient steps with learning rate alpha

    # Initialize some useful values
    m = len(y) # number of training examples
    J_history = np.zeros((num_iters, 1))

    for i in xrange(num_iters):

        # ====================== YOUR CODE HERE ======================
        # Instructions: Perform a single gradient step on the parameter vector
        #               theta. 
        #
        # Hint: While debugging, it can be useful to print out the values
        #       of the cost function (computeCost) and gradient here.
        #

        theta = theta - alpha*(1.0/m) * np.transpose(X).dot(X.dot(theta) - np.transpose([y]))

        # ============================================================

        # Save the cost J in every iteration    
        J_history[i] = computeCost(X, y, theta)
        # print(J_history[i])

    return theta, J_history

cur.execute("SELECT amount, price, p_fiveSec_avgPrice, p_fiveSec_highPrice, p_fiveSec_lowPrice, p_fiveSec_startPrice, p_fiveSec_endPrice, p_fiveSec_changeReal, p_fiveSec_changePercent, p_fiveSec_travelReal, p_fiveSec_travelPercent, p_fiveSec_volumePrMin, p_fiveSec_tradesPrMin, p_fiveSec_buysPrMin, p_fiveSec_sellsPrMin, p_tenSec_avgPrice, p_tenSec_highPrice, p_tenSec_lowPrice, p_tenSec_startPrice, p_tenSec_endPrice, p_tenSec_changeReal, p_tenSec_changePercent, p_tenSec_travelReal, p_tenSec_travelPercent, p_tenSec_volumePrMin, p_tenSec_tradesPrMin, p_tenSec_buysPrMin, p_tenSec_sellsPrMin, p_thirtySec_avgPrice, p_thirtySec_highPrice, p_thirtySec_lowPrice, p_thirtySec_startPrice, p_thirtySec_endPrice, p_thirtySec_changeReal, p_thirtySec_changePercent, p_thirtySec_travelReal, p_thirtySec_travelPercent, p_thirtySec_volumePrMin, p_thirtySec_tradesPrMin, p_thirtySec_buysPrMin, p_thirtySec_sellsPrMin, p_oneMin_avgPrice, p_oneMin_highPrice, p_oneMin_lowPrice, p_oneMin_startPrice, p_oneMin_endPrice, p_oneMin_changeReal, p_oneMin_changePercent, p_oneMin_travelReal, p_oneMin_travelPercent, p_oneMin_volumePrMin, p_oneMin_tradesPrMin, p_oneMin_buysPrMin, p_oneMin_sellsPrMin, p_threeMin_avgPrice, p_threeMin_highPrice, p_threeMin_lowPrice, p_threeMin_startPrice, p_threeMin_endPrice, p_threeMin_changeReal, p_threeMin_changePercent, p_threeMin_travelReal, p_threeMin_travelPercent, p_threeMin_volumePrMin, p_threeMin_tradesPrMin, p_threeMin_buysPrMin, p_threeMin_sellsPrMin, p_fiveMin_avgPrice, p_fiveMin_highPrice, p_fiveMin_lowPrice, p_fiveMin_startPrice, p_fiveMin_endPrice, p_fiveMin_changeReal, p_fiveMin_changePercent, p_fiveMin_travelReal, p_fiveMin_travelPercent, p_fiveMin_volumePrMin, p_fiveMin_tradesPrMin, p_fiveMin_buysPrMin, p_fiveMin_sellsPrMin, p_tenMin_avgPrice, p_tenMin_highPrice, p_tenMin_lowPrice, p_tenMin_startPrice, p_tenMin_endPrice, p_tenMin_changeReal, p_tenMin_changePercent, p_tenMin_travelReal, p_tenMin_travelPercent, p_tenMin_volumePrMin, p_tenMin_tradesPrMin, p_tenMin_buysPrMin, p_tenMin_sellsPrMin, p_fifteenMin_avgPrice, p_fifteenMin_highPrice, p_fifteenMin_lowPrice, p_fifteenMin_startPrice, p_fifteenMin_endPrice, p_fifteenMin_changeReal, p_fifteenMin_changePercent, p_fifteenMin_travelReal, p_fifteenMin_travelPercent, p_fifteenMin_volumePrMin, p_fifteenMin_tradesPrMin, p_fifteenMin_buysPrMin, p_fifteenMin_sellsPrMin, p_thirtyMin_avgPrice, p_thirtyMin_highPrice, p_thirtyMin_lowPrice, p_thirtyMin_startPrice, p_thirtyMin_endPrice, p_thirtyMin_changeReal, p_thirtyMin_changePercent, p_thirtyMin_travelReal, p_thirtyMin_travelPercent, p_thirtyMin_volumePrMin, p_thirtyMin_tradesPrMin, p_thirtyMin_buysPrMin, p_thirtyMin_sellsPrMin, p_sixtyMin_avgPrice, p_sixtyMin_highPrice, p_sixtyMin_lowPrice, p_sixtyMin_startPrice, p_sixtyMin_endPrice, p_sixtyMin_changeReal, p_sixtyMin_changePercent, p_sixtyMin_travelReal, p_sixtyMin_travelPercent, p_sixtyMin_volumePrMin, p_sixtyMin_tradesPrMin, p_sixtyMin_buysPrMin, p_sixtyMin_sellsPrMin, p_oneTwentyMin_avgPrice, p_oneTwentyMin_highPrice, p_oneTwentyMin_lowPrice, p_oneTwentyMin_startPrice, p_oneTwentyMin_endPrice, p_oneTwentyMin_changeReal, p_oneTwentyMin_changePercent, p_oneTwentyMin_travelReal, p_oneTwentyMin_travelPercent, p_oneTwentyMin_volumePrMin, p_oneTwentyMin_tradesPrMin, p_oneTwentyMin_buysPrMin, p_oneTwentyMin_sellsPrMin, f_thirtySec_endPrice, f_fifteenMin_endPrice, f_oneTwentyMin_endPrice FROM trades WHERE market = 'BTCUSD' AND f_thirtySec_endPrice != 0 AND f_fifteenMin_endPrice != 0 AND f_oneTwentyMin_endPrice != 0 LIMIT 100")
rows = cur.fetchall()

X = np.zeros((100,145))
y = np.zeros((100,3))
theta = np.random.rand(145,1)
i = 0
for row in rows:
	X[i] = np.array(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23],row[24],row[25],row[26],row[27],row[28],row[29],row[30],row[31],row[32],row[33],row[34],row[35],row[36],row[37],row[38],row[39],row[40],row[41],row[42],row[43],row[44],row[45],row[46],row[47],row[48],row[49],row[50],row[51],row[52],row[53],row[54],row[55],row[56],row[57],row[58],row[59],row[60],row[61],row[62],row[63],row[64],row[65],row[66],row[67],row[68],row[69],row[70],row[71],row[72],row[73],row[74],row[75],row[76],row[77],row[78],row[79],row[80],row[81],row[82],row[83],row[84],row[85],row[86],row[87],row[88],row[89],row[90],row[91],row[92],row[93],row[94],row[95],row[96],row[97],row[98],row[99],row[100],row[101],row[102],row[103],row[104],row[105],row[106],row[107],row[108],row[109],row[110],row[111],row[112],row[113],row[114],row[115],row[116],row[117],row[118],row[119],row[120],row[121],row[122],row[123],row[124],row[125],row[126],row[127],row[128],row[129],row[130],row[131],row[132],row[133],row[134],row[135],row[136],row[137],row[138],row[139],row[140],row[141],row[142],row[143],row[144])
	y30[i] = row[145]
	y15[i] = row[146]
	y120[i] = row[147]
print("Data Aquired, running gradietn decent")
alpha = 1
num_iters = 5
theta, J_history = gradientDescentMulti(X, y30, theta, alpha, num_iters)











