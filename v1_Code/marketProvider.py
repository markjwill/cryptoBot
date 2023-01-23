#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created by bu on 2018-01-17
"""
from __future__ import unicode_literals
import time
import hashlib
import json as complex_json
import urllib3
from datetime import datetime, timedelta
from urllib3.exceptions import InsecureRequestWarning
import ssl
import sys
import mariadb
import pprint
from pytz import timezone
import math
import numpy as np
import redis
import pickle


import liveCruncher
import categories

import credentials





def isOverPrediction(predictionSet):
    if predictionSet['value'] > 0:
        return True
    return False

def isUnderPrediction(predictionSet):
    if predictionSet['value'] < 0:
        return True
    return False

def isStillValid(predictionSet):
    if predictionSet['expires'] > datetime.now():
        return True
    return False

def isValidBetween(fromMin, toMin):
    def validCheck(predictionSet):
        if ( predictionSet['expires'] > datetime.now() + timedelta(minutes=fromMin) 
            and predictionSet['expires'] < datetime.now() + timedelta(minutes=toMin) ):
            return True
        return False
    return validCheck

isValidZeroOne = isValidBetween(0, 1)
isValidOneTwo = isValidBetween(1, 2)
isValidTwoThree = isValidBetween(2, 3)
isValidThreeFour = isValidBetween(3, 4)
isValidFourFive = isValidBetween(4, 5)

isValidZeroFifteen = isValidBetween(0, 15)
isValidFifteenThirty = isValidBetween(15, 30)

isValidZeroThirty = isValidBetween(0, 30)
isValidThirtySixty = isValidBetween(30, 60)
isValidSixtyNinety = isValidBetween(60, 90)
isValidNinetyOneTwenty = isValidBetween(90, 120)

class marketProvider:

  r = redis.StrictRedis(host='localhost',port=6377,db=0)

  avgSets = []
  predictionString = ''

  def __init__(self, buyCoin, sellCoin, marketSource):
    self.marketSource = marketSource
    self.buyCoin = buyCoin
    self.sellCoin = sellCoin
    self.market=self.buyCoin + self.sellCoin
    # Connect to MariaDB Platform
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

  def getLatestData(self):
    self.getLatestTrades()


  def getLatestTrades(self):
    # global callTime
    # request_client = RequestClient()
    # params = {
    #     'market': self.market,
    #     'limit': limit
    # }
    # response = request_client.request(
    #         'GET',
    #         '{url}/v1/market/deals'.format(url=request_client.url),
    #         params=params
    # )
    # try:
    #     latest = complex_json.loads(response.data)
    #     # print(complex_json.dumps(latest, indent = 4, sort_keys=True))
    # except AttributeError:
    #     print("Internet connectivity error, get_latest", flush=True)
    #     exit()

    latest = self.marketSource.get_latest(self.market)

    addTrades(latest, trader)

    self.currentPrice = latest[0]['price']

  def getLatestPrice(self, market):
    latest = self.marketSource.get_latest(market, 1)

    return latest[0]['price']


  def addTrades(self, newTrades):
    values = ''
    for trade in newTrades:
      values += '({0:d},"{1:s}",{2:d},{3:d},"{4:s}","{5:s}","{6:s}"),'.format(trade['id'], trade['amount'], trade['date'], trade['date_ms'], trade['price'], trade['type'], self.market)
    cur.execute(
      "INSERT IGNORE INTO {0:s}(id, amount, date, date_ms, price, type, market) VALUES ".format(self.dbTable) + values[:-1])
    conn.commit()
    (trades, tSecAvg, fiveAvg, thirtyAvg, oneTwentyAvg, fourEightyAvg) = liveCruncher.getLatestNumbers(self.trades)
    self.trades = trades
    self.latestTrade = self.trades[0]
    self.avgSets.append({
      'tSecAvg' : float(tSecAvg),
      'fiveAvg' : float(fiveAvg),
      'thirtyAvg' : float(thirtyAvg),
      'oneTwentyAvg' : float(oneTwentyAvg),
      'fourEightyAvg' : float(fourEightyAvg)
    })
    self.r.lpush(self.avgSetsQueue, pickle.dumps({
      'tSecAvg' : float(tSecAvg),
      'fiveAvg' : float(fiveAvg),
      'thirtyAvg' : float(thirtyAvg),
      'oneTwentyAvg' : float(oneTwentyAvg),
      'fourEightyAvg' : float(fourEightyAvg)
    }))









    currentPrice = 0.0
    previousCurrentPrice=1.0

    tSecPrediction = 0.0
    fivePrediction = 0.0
    thirtyPrediction = 0.0
    oneTwentyPrediction = 0.0

    tSecPredictionSet = []
    fivePredictionSet = []
    thirtyPredictionSet = []
    oneTwentyPredictionSet = []

    predictionSpreads = {}

    trades = []

    market = ''
    buyCoin = ''
    sellCoin = ''
    dbTable = ''


    tSecPrediction = 0.0
    fivePrediction = 0.0
    thirtyPrediction = 0.0
    oneTwentyPrediction = 0.0
    tSecPredictionSet = []
    fivePredictionSet = []
    thirtyPredictionSet = []
    oneTwentyPredictionSet = []
    predictionSpreads = {}

    # If buyCoin is worth more, lean > 1.0
    # If sellCoin is worth more, lean < 1.0
    USDTlean=1.0
    BTClean=1.0
    ETHLean=1.0
    BCHlean=1.0
    currencyLean=1.0 

    currencyPrices = {}

    avgSetsQueue = ''
    tSecPredictionSetQueue = ''
    fivePredictionSetQueue = ''
    thirtyPredictionSetQueue = ''
    oneTwentyPredictionSetQueue = ''


    def __init__(self, buyCoin, sellCoin, dataSource):
        self.buyCoin = buyCoin
        self.sellCoin = sellCoin
        self.market = self.buyCoin + self.sellCoin
        self.dataSource = dataSource
        self.dbTable = self.dataSource.name+'_'+self.market

        self.avgSetsQueue = 'avgSets_'+self.dbTable
        self.tSecPredictionSetQueue = 'tSecPredictionSet_'+self.dbTable
        self.fivePredictionSetQueue = 'fivePredictionSet_'+self.dbTable
        self.thirtyPredictionSetQueue = 'thirtyPredictionSet_'+self.dbTable
        self.oneTwentyPredictionSetQueue = 'oneTwentyPredictionSet_'+self.dbTable
        # Connect to MariaDB Platform
        try:
            self.conn = mariadb.connect(
                user="MrBot",
                password="8fdvaoivposriong",
                host="127.0.0.1",
                port=3306,
                database="botscape"

            )
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)

        # Get Cursor
        self.cur = self.conn.cursor()


    def updatePriceAndPredictions():
        self.getLatestTrades()
        self.calculateCurrencyLean()
        self.predictPrice()

    def getPriceChangeSinceLast(self):
        self.priceChange = self.previousCurrentPrice - self.currentPrice
        self.percentChange = self.priceChange * 100 / self.previousCurrentPrice
        self.previousCurrentPrice = self.currentPrice


    def getAveragePriceSets():
        while self.r.llen(self.avgSetsQueue) > 5:
            # remove and set last item until there are 5
            oldSet = pickle.loads(self.r.rpop(self.avgSetsQueue))

        #set the newest item
        newSet = pickle.loads(self.r.lindex(self.avgSetsQueue, 0))
        return newSet, oldSet

    def calculateCurrentSlopes(self, newSet, oldSet):
        self.tSecSlope = newSet['tSecAvg'] - oldSet['tSecAvg']
        self.fiveSlope = newSet['fiveAvg'] - oldSet['fiveAvg']
        self.thirtySlope = newSet['thirtyAvg'] - oldSet['thirtyAvg']
        self.oneTwentySlope = newSet['oneTwentyAvg'] - oldSet['oneTwentyAvg']

    def predictPrice(self):

        newSet, oldSet = self.getAveragePriceSets()

        if self.r.llen(self.avgSetsQueue) == 5:

            self.calculateCurrentSlopes(newSet, oldSet)

            averagesString = categories.getRollingString(self.currentPrice, newSet['tSecAvg'], newSet['fiveAvg'], newSet['thirtyAvg'], newSet['oneTwentyAvg'])
            slopesString = categories.getSlopeString(self.tSecSlope, self.fiveSlope, self.thirtySlope, self.oneTwentySlope)
            self.predictionString = averagesString+'-'+slopesString
            cur.execute('SELECT tSecValue, fiveValue, thirtyValue, oneTwentyValue FROM predictions WHERE predictions.key = "{0:s}"'.format(self.predictionString))
            values = cur.fetchall()

            self.tSecPrediction = 0.0
            self.fivePrediction = 0.0
            self.thirtyPrediction = 0.0
            self.oneTwentyPrediction = 0.0
            for value in values:
                self.tSecPrediction = value[0]
                self.fivePrediction = value[1]
                self.thirtyPrediction = value[2]
                self.oneTwentyPrediction = value[3]
            now = datetime.now()
            tSecExpire = now + timedelta(seconds=30)
            fiveExpire = now + timedelta(minutes=5)
            thirtyExpire = now + timedelta(minutes=30)
            oneTwentyExpire = now + timedelta(minutes=120)


            while self.r.llen(self.tSecPredictionSetQueue) > 1:
                self.r.rpop(self.tSecPredictionSetQueue)
            self.tSecPredictionSet = pickle.loads(self.r.lindex(self.tSecPredictionSetQueue, 0))
            if self.tSecPrediction != 0.0:
                self.tSecPredictionSet.append({
                    'value' : self.tSecPrediction,
                    'price' : self.predictionToPrice(self.tSecPrediction),
                    'expires' : tSecExpire
                })
            self.tSecPredictionSet = list(filter(isStillValid, self.tSecPredictionSet))
            self.r.lpush(self.tSecPredictionSetQueue, pickle.dumps(self.tSecPredictionSet))
            tSecUnderSet = list(filter(isUnderPrediction, self.tSecPredictionSet))
            tSecOverSet = list(filter(isOverPrediction, self.tSecPredictionSet))
            self.predictionSpreads['tSec'] = {}
            self.predictionSpreads['tSec']['Over'] = self.calculateSpread(tSecOverSet)
            self.predictionSpreads['tSec']['Under'] = self.calculateSpread(tSecUnderSet)


            while self.r.llen(self.fivePredictionSetQueue) > 1:
                self.r.rpop(self.fivePredictionSetQueue)
            self.fivePredictionSet = pickle.loads(self.r.lindex(self.fivePredictionSetQueue, 0))
            if self.fivePrediction != 0.0:
                self.fivePredictionSet.append({
                    'value' : self.fivePrediction,
                    'price' : self.predictionToPrice(self.fivePrediction),
                    'expires' : fiveExpire
                })
            self.fivePredictionSet = list(filter(isStillValid, self.fivePredictionSet))
            self.r.lpush(self.fivePredictionSetQueue, pickle.dumps(self.fivePredictionSet))

            fourFivePredictionSet = list(filter(isValidFourFive, self.fivePredictionSet))
            fourFiveUnderSet = list(filter(isUnderPrediction, fourFivePredictionSet))
            fourFiveOverSet = list(filter(isOverPrediction, fourFivePredictionSet))
            self.predictionSpreads['fourFive'] = {}
            self.predictionSpreads['fourFive']['Over'] = self.calculateSpread(fourFiveOverSet)
            self.predictionSpreads['fourFive']['Under'] = self.calculateSpread(fourFiveUnderSet)

            threeFourPredictionSet = list(filter(isValidThreeFour, self.fivePredictionSet))
            threeFourUnderSet = list(filter(isUnderPrediction, threeFourPredictionSet))
            threeFourOverSet = list(filter(isOverPrediction, threeFourPredictionSet))
            self.predictionSpreads['threeFour'] = {}
            self.predictionSpreads['threeFour']['Over'] = self.calculateSpread(threeFourOverSet)
            self.predictionSpreads['threeFour']['Under'] = self.calculateSpread(threeFourUnderSet)

            twoThreePredictionSet = list(filter(isValidTwoThree, self.fivePredictionSet))
            twoThreeUnderSet = list(filter(isUnderPrediction, twoThreePredictionSet))
            twoThreeOverSet = list(filter(isOverPrediction, twoThreePredictionSet))
            self.predictionSpreads['twoThree'] = {}
            self.predictionSpreads['twoThree']['Over'] = self.calculateSpread(twoThreeOverSet)
            self.predictionSpreads['twoThree']['Under'] = self.calculateSpread(twoThreeUnderSet)

            oneTwoPredictionSet = list(filter(isValidOneTwo, self.fivePredictionSet))
            oneTwoUnderSet = list(filter(isUnderPrediction, oneTwoPredictionSet))
            oneTwoOverSet = list(filter(isOverPrediction, oneTwoPredictionSet))
            self.predictionSpreads['oneTwo'] = {}
            self.predictionSpreads['oneTwo']['Over'] = self.calculateSpread(oneTwoOverSet)
            self.predictionSpreads['oneTwo']['Under'] = self.calculateSpread(oneTwoUnderSet)

            zeroOnePredictionSet = list(filter(isValidZeroOne, self.fivePredictionSet))
            zeroOneUnderSet = list(filter(isUnderPrediction, zeroOnePredictionSet))
            zeroOneOverSet = list(filter(isOverPrediction, zeroOnePredictionSet))
            self.predictionSpreads['zeroOne'] = {}
            self.predictionSpreads['zeroOne']['Over'] = self.calculateSpread(zeroOneOverSet)
            self.predictionSpreads['zeroOne']['Under'] = self.calculateSpread(zeroOneUnderSet)

            fiveUnderSet = list(filter(isUnderPrediction, self.fivePredictionSet))
            fiveOverSet = list(filter(isOverPrediction, self.fivePredictionSet))
            self.predictionSpreads['five'] = {}
            self.predictionSpreads['five']['Over'] = self.calculateSpread(fiveOverSet)
            self.predictionSpreads['five']['Under'] = self.calculateSpread(fiveUnderSet)



            while self.r.llen(self.thirtyPredictionSetQueue) > 1:
                self.r.rpop(self.thirtyPredictionSetQueue)
            self.thirtyPredictionSet = pickle.loads(self.r.lindex(self.thirtyPredictionSetQueue, 0))
            if self.thirtyPrediction != 0.0:
                self.thirtyPredictionSet.append({
                    'value' : self.thirtyPrediction,
                    'price' : self.predictionToPrice(self.thirtyPrediction),
                    'expires' : thirtyExpire
                })
            self.thirtyPredictionSet = list(filter(isStillValid, self.thirtyPredictionSet))
            self.r.lpush(self.thirtyPredictionSetQueue, pickle.dumps(self.thirtyPredictionSet))
            zeroFifteenPredictionSet = list(filter(isValidZeroFifteen, self.thirtyPredictionSet))
            fifteenThirtyPredictionSet = list(filter(isValidFifteenThirty, self.thirtyPredictionSet))
            
            fifteenThirtyUnderSet = list(filter(isUnderPrediction, fifteenThirtyPredictionSet))
            fifteenThirtyOverSet = list(filter(isOverPrediction, fifteenThirtyPredictionSet))
            self.predictionSpreads['fifteenThirty'] = {}
            self.predictionSpreads['fifteenThirty']['Over'] = self.calculateSpread(fifteenThirtyOverSet)
            self.predictionSpreads['fifteenThirty']['Under'] = self.calculateSpread(fifteenThirtyUnderSet)

            zeroFifteenUnderSet = list(filter(isUnderPrediction, zeroFifteenPredictionSet))
            zeroFifteenOverSet = list(filter(isOverPrediction, zeroFifteenPredictionSet))
            self.predictionSpreads['zeroFifteen'] = {}
            self.predictionSpreads['zeroFifteen']['Over'] = self.calculateSpread(zeroFifteenOverSet)
            self.predictionSpreads['zeroFifteen']['Under'] = self.calculateSpread(zeroFifteenUnderSet)


            while self.r.llen(self.oneTwentyPredictionSetQueue) > 1:
                self.r.rpop(self.oneTwentyPredictionSetQueue)
            self.oneTwentyPredictionSet = pickle.loads(self.r.lindex(self.oneTwentyPredictionSetQueue, 0))
            if self.oneTwentyPrediction != 0.0:
                self.oneTwentyPredictionSet.append({
                    'value' : self.oneTwentyPrediction,
                    'price' : self.predictionToPrice(self.oneTwentyPrediction),
                    'expires' : oneTwentyExpire
                })
            self.oneTwentyPredictionSet = list(filter(isStillValid, self.oneTwentyPredictionSet))
            self.r.lpush(self.oneTwentyPredictionSetQueue, pickle.dumps(self.oneTwentyPredictionSet))

            zeroThirtySet = list(filter(isValidZeroThirty, self.oneTwentyPredictionSet))
            thirtySixtySet = list(filter(isValidThirtySixty, self.oneTwentyPredictionSet))
            sixtyNinetySet = list(filter(isValidSixtyNinety, self.oneTwentyPredictionSet))
            ninetyOneTwentySet = list(filter(isValidNinetyOneTwenty, self.oneTwentyPredictionSet))

            ninetyOneTwentyUnderSet = list(filter(isUnderPrediction, ninetyOneTwentySet))
            ninetyOneTwentyOverSet = list(filter(isOverPrediction, ninetyOneTwentySet))
            self.predictionSpreads['ninetyOneTwenty'] = {}
            self.predictionSpreads['ninetyOneTwenty']['Over'] = self.calculateSpread(ninetyOneTwentyOverSet)
            self.predictionSpreads['ninetyOneTwenty']['Under'] = self.calculateSpread(ninetyOneTwentyUnderSet)

            sixtyNinetyUnderSet = list(filter(isUnderPrediction, sixtyNinetySet))
            sixtyNinetyOverSet = list(filter(isOverPrediction, sixtyNinetySet))
            self.predictionSpreads['sixtyNinety'] = {}
            self.predictionSpreads['sixtyNinety']['Over'] = self.calculateSpread(sixtyNinetyOverSet)
            self.predictionSpreads['sixtyNinety']['Under'] = self.calculateSpread(sixtyNinetyUnderSet)

            thirtySixtyUnderSet = list(filter(isUnderPrediction, thirtySixtySet))
            thirtySixtyOverSet = list(filter(isOverPrediction, thirtySixtySet))
            self.predictionSpreads['thirtySixty'] = {}
            self.predictionSpreads['thirtySixty']['Over'] = self.calculateSpread(thirtySixtyOverSet)
            self.predictionSpreads['thirtySixty']['Under'] = self.calculateSpread(thirtySixtyUnderSet)
            
            zeroThirtyUnderSet = list(filter(isUnderPrediction, zeroThirtySet))
            zeroThirtyOverSet = list(filter(isOverPrediction, zeroThirtySet))
            self.predictionSpreads['zeroThirty'] = {}
            self.predictionSpreads['zeroThirty']['Over'] = self.calculateSpread(zeroThirtyOverSet)
            self.predictionSpreads['zeroThirty']['Under'] = self.calculateSpread(zeroThirtyUnderSet)            

    def calculateSpread(self, tSet):
        spread = {
            'min' : self.currentPrice,
            'mid' : self.currentPrice,
            'avg' : self.currentPrice,
            'max' : self.currentPrice,
            'std' : 0.0,
            'betAvg' : self.currentPrice,
            'count' : len(tSet)
        }

        if spread['count']:
            tSet.sort(key=lambda item: item['price'] )
            totVal = sum(p['price'] for p in tSet)
            spread['avg'] = totVal / spread['count']
            spread['min'] = tSet[0]['price']
            spread['mid'] = tSet[round(spread['count']/2)]['price']
            spread['max'] = tSet[-1]['price']
            spread['std'] = np.std([p['price'] for p in tSet])
            if spread['avg'] > self.currentPrice:
                spread['betAvg'] = max(self.currentPrice, (spread['avg'] - spread['std']))
            else:
                spread['betAvg'] = min(self.currentPrice, (spread['avg'] + spread['std']))

        return spread

    def priceToPrediction(self, price):
        prediction = ( price / self.currentPrice * 100 ) - 100
        if prediction == -100:
            return 0.0
        return prediction

    def predictionToPrice(self, prediction):
        return self.currentPrice * (( prediction + 100 ) / 100)

    def calculateCurrencyLean(self, mainData):
        self.USDTlean=1.0
        if self.sellCoin != 'USDT' and self.buyCoin != 'USDT':
            sellUSDData = get_latest(self, self.sellCoin + 'USDT', 1)
            self.currencyPrices[self.sellCoin] = sellUSDData['currentPrice']
            buyUSDData = get_latest(self, self.buyCoin + 'USDT', 1)
            self.currencyPrices[self.buyCoin] = buyUSDData['currentPrice']
            self.USDTlean = ( buyUSDData['currentPrice'] / sellUSDData['currentPrice'] ) / mainData['currentPrice']
        if self.USDTlean < 0.0000001:
            self.USDTlean = 1.0

        self.BTClean=1.0
        if self.sellCoin != 'BTC' and self.buyCoin != 'BTC':
            market = self.sellCoin + 'BTC'
            if self.sellCoin == 'USDT':
                market = 'BTC'+self.sellCoin
            sellBTCData = get_latest(self, market, 1)
            if self.sellCoin == 'USDT':
                sellBTCData['currentPrice'] = 1.0 / sellBTCData['currentPrice']
            market = self.buyCoin + 'BTC'
            if self.buyCoin == 'USDT':
                market = 'BTC'+self.buyCoin
            buyBTCData = get_latest(self, market, 1)
            if self.buyCoin == 'USDT':
                buyBTCData['currentPrice'] = 1.0 / buyBTCData['currentPrice']
            self.BTClean = ( buyBTCData['currentPrice'] / sellBTCData['currentPrice'] ) / mainData['currentPrice']
        if self.BTClean < 0.0000001:
            self.BTClean = 1.0

        self.ETHlean=1.0
        if self.sellCoin != 'ETH' and self.buyCoin != 'ETH':
            market = self.sellCoin + 'ETH'
            if self.sellCoin == 'USDT' or self.sellCoin == 'BTC':
                market = 'ETH'+self.sellCoin
            sellETHData = get_latest(self, market, 1)
            if self.sellCoin == 'USDT' or self.sellCoin == 'BTC':
                sellETHData['currentPrice'] = 1.0 / sellETHData['currentPrice']
            market = self.buyCoin + 'ETH'
            if self.buyCoin == 'USDT' or self.buyCoin == 'BTC':
                market = 'ETH'+self.buyCoin
            buyETHData = get_latest(self, market, 1)
            if self.buyCoin == 'USDT' or self.buyCoin == 'BTC':
                buyETHData['currentPrice'] = 1.0 / buyETHData['currentPrice']
            self.ETHlean = ( buyETHData['currentPrice'] / sellETHData['currentPrice'] ) / mainData['currentPrice']
        if self.ETHlean < 0.0000001:
            self.ETHlean = 1.0

        self.BCHlean=1.0
        if self.sellCoin != 'BCH' and self.buyCoin != 'BCH':
            market = self.sellCoin + 'BCH'
            if self.sellCoin == 'USDT' or self.sellCoin == 'BTC':
                market = 'BCH'+self.sellCoin
            sellBCHData = get_latest(self, market, 1)
            if self.sellCoin == 'USDT' or self.sellCoin == 'BTC':
                sellBCHData['currentPrice'] = 1.0 / sellBCHData['currentPrice']
            market = self.buyCoin + 'BCH'
            if self.buyCoin == 'USDT' or self.buyCoin == 'BTC':
                market = 'BCH'+self.buyCoin
            buyBCHData = get_latest(self, market, 1)
            if self.buyCoin == 'USDT' or self.buyCoin == 'BTC':
                buyBCHData['currentPrice'] = 1.0 / buyBCHData['currentPrice']
            self.BCHlean = ( buyBCHData['currentPrice'] / sellBCHData['currentPrice'] ) / mainData['currentPrice']
        if self.BCHlean < 0.0000001:
            self.BCHlean = 1.0

        currencyLean = (self.BTClean + self.USDTlean + self.ETHlean + self.BCHlean) / 4

        if self.nextActionType() == 'buy':
            if currencyLean > 1.0:
                self.currencyLean = 1 - ( currencyLean - 1 )
            else:
                self.currencyLean = ( 1 - currencyLean ) + 1
        else:
            if currencyLean > 1.0:
                self.currencyLean = ( 1 - currencyLean ) + 1
            else:
                self.currencyLean = 1 - ( currencyLean - 1 )




