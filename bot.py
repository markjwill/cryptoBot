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
from ast import literal_eval

import liveCruncher
import categories

import credentials

# cur.execute(
#     "SELECT first_name,last_name FROM employees WHERE first_name=?",
#     (some_name,))

# for (first_name, last_name) in cur:
#     print(f"First Name: {first_name}, Last Name: {last_name}")

# cur.execute(
#     "INSERT INTO employees (first_name,last_name) VALUES (?, ?)",
#     (first_name, last_name))

# cur.execute(
#     "INSERT INTO history (time, currentPrice, actionTaken, market, tradeId, avgActionPrice, buyActionPrice, sellActionPrice) VALUES (?,?,?,?,?,?,?,?)",
#     (time, currentPrice, actionTaken, market, tradeId, avgActionPrice, buyActionPrice, sellActionPrice))


# Auto Hard Reset After 1 day of no trades
# Add BCHUSDT to XMRUSDT factor
# Add BCHBTC to XMRBTC factor
# Make action percents dynamic

urllib3.disable_warnings(InsecureRequestWarning)
http = urllib3.PoolManager(timeout=urllib3.Timeout(connect=1, read=2))

orderUp=False

resetValue = 'reset'
if len(sys.argv) > 1:
    if sys.argv[1] == 'hard':
        resetValue = 'hardReset'

buyCoin='BTC'
sellCoin='USDT'


currentPrice=0
Amount=0
Remaining=0

callTime = {
    'wait' : 3,
    'green' : 3,
    'red' : 3,
    'in-progress' : 5,
    'first': 3
}

try:
    conn = mariadb.connect(
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
cur = conn.cursor()

class RequestClient(object):
    __headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
    }

    def __init__(self, headers={}):
        self.access_id = credentials.coinExAccessId
        self.secret_key = credentials.coinExSecretKey
        self.url = 'https://api.coinex.com'
        self.headers = self.__headers
        self.headers.update(headers)

    @staticmethod
    def get_sign(params, secret_key):
        sort_params = sorted(params)
        data = []
        for item in sort_params:
            data.append(item + '=' + str(params[item]))
        str_params = "{0}&secret_key={1}".format('&'.join(data), secret_key)
        token = hashlib.md5(str_params.encode('utf-8')).hexdigest().upper()
        return token

    def set_authorization(self, params):
        params['access_id'] = self.access_id
        params['tonce'] = int(time.time()*1000)
        self.headers['AUTHORIZATION'] = self.get_sign(params, self.secret_key)

    def request(self, method, url, params={}, data='', json={}):
        try:
            method = method.upper()
            if method in ['GET', 'DELETE']:
                self.set_authorization(params)
                result = http.request(method, url, fields=params, headers=self.headers)
            else:
                if data:
                    json.update(complex_json.loads(data))
                self.set_authorization(json)
                encoded_data = complex_json.dumps(json).encode('utf-8')
                result = http.request(method, url, body=encoded_data, headers=self.headers)
            return result
        except urllib3.exceptions.MaxRetryError:
            print("Internet connectivity error")
        except urllib3.exceptions.NewConnectionError:
            print("Internet connectivity error")
        except OSError:
            print("Internet connectivity error")

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

# def isStillValidSixty(predictionSet):
#     if ( predictionSet['expires'] > datetime.now() + timedelta(minutes=90) ):
#         return True
#     return False

# def isStillValidNinety(predictionSet):
#     if ( predictionSet['expires'] > datetime.now() + timedelta(minutes=60)
#         and predictionSet['expires'] < datetime.now() + timedelta(minutes=90) ):
#         return True
#     return False

# def isStillValidOneTwenty(predictionSet):
#     if ( predictionSet['expires'] < datetime.now() + timedelta(minutes=60)):
#         return True
#     return False

class Bot:

    mode='wait'
    lastAction='reset'
    lastPrice=0.0 # price most recently completed in a trade
    lastTime=datetime.now()
    listedPrice=0.0 # price most recently placed in a trade
    listedTime=datetime.now()
    listedType='none'
    lastestPrice=0.0 # current price farthest toward profit

    previousCurrentPrice=1.0

    greenPrice=0.0 # minimum price to trade for profit ( Does Not Move )
    guidePrice=0.0 # price farthest toward profit since last action ( Moves )
    actionPrice=0.0 # price we will sell at post hook ( Moves )
                    # equal to guide minus fee or greenPrice
    goalPrice=0.0 # price we will sit at pre hook ( Moves )

    tradeInProgress=False

    goalTime=datetime.now()
    goalTimeIncriment=5
    goalTimeSince=0
    actionTime=datetime.now()

    goalPercent=15
    goalPercentReset=15
    goalPercentTime=datetime.now()
    guidePercent=1.0
    actionPercent=0.5

    sellNowMovement=0.75

    USDTlean=1.0
    usdtPrice=0.0
    BTClean=1.0
    btcPrice=0.0
    ETHlean=1.0
    ethPrice=0.0
    BCHlean=1.0
    bchPrice=0.0
    currencyLean=1.0 # If buyCoin is worth more, lean > 1.0
             # If sellCoin is worth more, lean < 1.0

    endDealMoney = 0.0
    endAmount = 0.0

    trades = []

    fSlopeMode = 'waiting'

    actionTaken=''
    actionValue=0.0
    completedTradeId=""

    timeLean=1.0

    historyLean=1.0
    resetHardTimeHours=3
    recentRangeLean=1.0
    resetTime=datetime.now()

    greenTouchesLean=1.0
    cetPile=10050.0
    cetBuyTime=datetime.now()
    cetId=0

    lowStartPercent=0.3
    highStartPercent=0.7

    feeAsPercent=0.14
    fee=0.00112
    startPeakFeeMultiple=2

    tradeId="0"
    remaining="0"
    amount="0"
    distanceToGreen=0.0

    latestPrice=0.0

    market=''
    buyCoin=''
    sellCoin=''

    debugLevel='prod'

    greenTouches=0
    redTouches=0

    available = {}
    frozen = {}

    newHistory = {}
    history = {}
    cetSellHistory = {}
    cetBuyHistory = {}
    currentPrice = {}

    avgSets = []
    predictionString = ''
    predictionValue = 0.0
    predictionLongValue = 0.0
    predictionSets = []
    avgPricePrediction = 0.0
    avgLongPricePrediction = 0.0
    avgPrediction = 0.0
    avgLongPrediction = 0.0
    avgPricePredictionSets = []
    minPrediction = {}
    midPrediction = {}
    maxPrediction = {}
    minLongPrediction = {}
    midLongPrediction = {}
    maxLongPrediction = {}
    avgLongPredictionValue = 0.0
    avgPredictionValue = 0.0

    tSecPrediction = 0.0
    fivePrediction = 0.0
    thirtyPrediction = 0.0
    oneTwentyPrediction = 0.0
    tSecPredictionSet = []
    fivePredictionSet = []
    thirtyPredictionSet = []
    oneTwentyPredictionSet = []
    predictionSpreads = {}

    lastFirstTrade = 0

    priceChange = 0.0
    percentChange = 0.0

    coinbasePrice = 0.0
    binancePrice = 0.0
    huobiPrice = 0.0
    # coingeckoPrice = 0.0
    bittrexPrice = 0.0

    pricePosition = 3

    nowTrendVeryDown=False
    nowTrendVeryUp=False
    nowTrendConsensus= False
    nowTrendAvg=0.0
    nowTrendStd=0.0
    nowTrendBetAvg=0.0
    nowTrendAllDown = False
    nowTrendAllUp = False

    tetherPrice = 1.0

    debugPrintRotation=0

    outsidePriceCounter=0

    priceData = {
        'sellActionPrice' : "0",
        'buyActionPrice' : "0",
        'avgActionPrice': 0.0,
        'timeRange': "0",
        'lowPrice': "0",
        'highPrice': "0",
        'currentPrice': "0"
    }

    twelveHourProfit = 0.0

    first = True
    second = True

    r = redis.StrictRedis(host='localhost',port=6377,db=0)

    def __init__(self, buyCoin, sellCoin, resetLevel):
        self.buyCoin = buyCoin
        self.sellCoin = sellCoin
        self.market=self.buyCoin + self.sellCoin
        self.lastAction=resetLevel
        self.actionTaken = ''
        while self.r.llen('lastRedTrade') > 0:
            self.r.rpop('lastRedTrade')
        self.frozen[self.buyCoin] = 0.0
        self.frozen[self.sellCoin] = 0.0
        self.available[self.sellCoin] = 0.0
        self.available[self.buyCoin] = 0.0
        self.available['CET'] = 0.0
        resetTime=datetime.now() - timedelta(hours=self.redTimeHours())
        try:
            self.cetSellHistory = order_finished('CET'+self.sellCoin, 1, 100, self)
        except KeyError:
            self.cetSellHistory = {}
        except AttributeError:
            self.cetSellHistory = {}
        try:
            self.cetBuyHistory = order_finished('CET'+self.buyCoin, 1, 100, self)
        except KeyError:
            self.cetBuyHistory = {}
        except AttributeError:
            self.cetBuyHistory = {}
        self.greenTouches = 0
        while self.r.llen('greenTouches') > 0:
            self.greenTouches = int(self.r.rpop('greenTouches'))

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

        if self.r.llen('lastRedTrade') == 0:
            cur.execute('SELECT finished_time FROM orders WHERE bot_type = "red" AND status = "done" ORDER BY id DESC LIMIT 1')
            values = cur.fetchall()
            for value in values:
                redTime = value[0]
            self.r.lpush('lastRedTrade', pickle.dumps(redTime))

    def getAccountValue(self):
        USDest = 0.0
        if ( ( self.toUSDRaw(self.available[self.sellCoin], self.sellCoin) > 10 and self.toUSDRaw(self.frozen[self.sellCoin], self.sellCoin) > 10 )
            or ( self.toUSDRaw(self.available[self.buyCoin], self.buyCoin) > 10 and self.toUSDRaw(self.frozen[self.buyCoin], self.buyCoin) > 10 ) ):
            get_account(self)

        USDest += self.toUSDRaw(self.available[self.sellCoin], self.sellCoin)
        USDest += self.toUSDRaw(self.frozen[self.sellCoin], self.sellCoin)
        USDest += self.toUSDRaw(self.available[self.buyCoin], self.buyCoin)
        USDest += self.toUSDRaw(self.frozen[self.buyCoin], self.buyCoin)

        USDest += self.toUSDRaw(self.available['CET'], 'CET')

        if ( ( self.toUSDRaw(self.available[self.sellCoin], self.sellCoin) > 10 and self.toUSDRaw(self.frozen[self.sellCoin], self.sellCoin) > 10 )
            or ( self.toUSDRaw(self.available[self.buyCoin], self.buyCoin) > 10 and self.toUSDRaw(self.frozen[self.buyCoin], self.buyCoin) > 10 ) ):
            print("Available {1:s} ${0:0.2f}".format(self.toUSDRaw(self.available[self.sellCoin], self.sellCoin), self.sellCoin))
            print("Available {1:s} ${0:0.2f}".format(self.toUSDRaw(self.available[self.buyCoin], self.buyCoin), self.buyCoin))
            print("Frozen {1:s} ${0:0.2f}".format(self.toUSDRaw(self.frozen[self.sellCoin], self.sellCoin), self.sellCoin))
            print("Frozen {1:s} ${0:0.2f}".format(self.toUSDRaw(self.frozen[self.buyCoin], self.buyCoin), self.buyCoin))
            print("Available {1:s} ${0:0.2f}".format(self.toUSDRaw(self.available['CET'], 'CET'), 'CET'))
            print("Total ${0:0.2f}".format(USDest), flush=True)
            print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), flush=True)
        return USDest


    def go(self):
        self.actionTaken=''
        self.actionValue=0.0
        self.completedTradeId=0
        self.priceData = get_latest(self, self.market, 200)
        self.getOutsidePrice()
        if self.checkOrderPending():
            self.mode = 'in-progress'
        else:
            self.calculateLean(self.priceData)
            self.getOutsidePrice()
            # pp.pprint(currentData['currentPrice'])
            self.predictPrice()
            self.addCurrentPrice(self.priceData)
            self.printDebug()
        self.getPriceChangeSinceLast()

        if self.first or self.second:
            self.mode = 'first'
            if self.first == False:
                self.second = False
            self.first = False

        if self.r.llen('lastRedTrade') == 0:
            cur.execute('SELECT finished_time FROM orders WHERE bot_type = "red" AND status = "done" ORDER BY id DESC LIMIT 1')
            values = cur.fetchall()
            for value in values:
                redTime = value[0]
            self.r.lpush('lastRedTrade', pickle.dumps(redTime))
        self.r.rpop('greenTouches')
        self.r.lpush('greenTouches', self.greenTouches)

    def getOutsidePrice(self):
        self.outsidePriceCounter += 1

        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
        }
        # if self.outsidePriceCounter == 1:
        try:
            params = {
                'contract_code': 'BTC-USDT',
            }
            result = http.request('GET', 'https://api.hbdm.com/linear-swap-api/v1/swap_index', params, headers)
            # print(result.data, flush=True)
            data = literal_eval(result.data.decode('utf8'))
            # print(complex_json.dumps(data, indent = 4, sort_keys=True), flush=True)
            self.huobiPrice = data['data'][0]['index_price']
        except:
            self.huobiPrice = 0.0

        # if self.outsidePriceCounter == 2:
        #     try:
        #         params = {
        #             'ids': 'bitcoin',
        #             'vs_currencies': 'USD'
        #         }
        #         result = http.request('GET', 'https://api.coingecko.com/api/v3/simple/price', params, headers)
        #         # print(result.data, flush=True)
        #         data = literal_eval(result.data.decode('utf8'))
        #         # print(complex_json.dumps(data, indent = 4, sort_keys=True), flush=True)
        #         self.coingeckoPrice = data['bitcoin']['usd'] / self.tetherPrice
        #     except:
        #         self.coingeckoPrice = 0.0

        # if self.outsidePriceCounter == 3:
        try:
            params = {
                'symbol': self.market,
            }
            result = http.request('GET', 'https://api1.binance.com/api/v3/ticker/price', params, headers)
            # print(result.data, flush=True)
            data = literal_eval(result.data.decode('utf8'))
            # print(complex_json.dumps(data, indent = 4, sort_keys=True), flush=True)
            self.binancePrice = float(data['price'])
        except:
            self.binancePrice = 0.0


        # https://api.kraken.com/0/public/Ticker?pair=tbtcusd

        try:
            params = {
                'market': 'USDT-BTC'
            }
            result = http.request('GET', 'https://api.bittrex.com/api/v1.1/public/getticker', params, headers)
            # print(result.data, flush=True)
            data = complex_json.loads(result.data.decode('utf8'))
            # print(complex_json.dumps(data, indent = 4, sort_keys=True), flush=True)
            self.bittrexPrice = data['result']['Last']
        except:
            self.bittrexPrice = 0.0

        # if self.outsidePriceCounter == 5:
        try:
            params = {}
            result = http.request('GET', 'https://api.pro.coinbase.com/products/BTC-USD/ticker', params, headers)
            data = literal_eval(result.data.decode('utf8'))
            # print(result.data, flush=True)
            # print(complex_json.dumps(data, indent = 4, sort_keys=True), flush=True)
            self.coinbasePrice = float(data['price']) / self.tetherPrice
        except:
            self.coinbasePrice = 0.0

        if self.outsidePriceCounter == 6:
            self.outsidePriceCounter = 0
            try:
                params = {
                    'market': 'USD-USDT'
                }
                result = http.request('GET', 'https://api.bittrex.com/api/v1.1/public/getticker', params, headers)
                # print(result.data, flush=True)
                data = complex_json.loads(result.data.decode('utf8'))
                # print(complex_json.dumps(data, indent = 4, sort_keys=True), flush=True)
                self.tetherPrice = data['result']['Last']
            except:
                self.tetherPrice = 1.0
        self.getNowIndicatorsSpread()


    def getNowIndicatorsSpread(self):
        prices = []
        self.nowTrendAllUp = True
        self.nowTrendAllDown = True
        if self.ETHlean != 1.0:
            prices.append(self.ethPrice)
        if self.BCHlean != 1.0:
            prices.append(self.bchPrice)
        if self.BTClean != 1.0:
            prices.append(self.btcPrice)
        if self.USDTlean != 1.0:
            prices.append(self.usdtPrice)
        if self.huobiPrice != 0.0:
            prices.append(self.huobiPrice)
        # if self.coingeckoPrice != 0.0:
        #     pices.append(self.coingeckoPrice)
        if self.binancePrice != 0.0:
            prices.append(self.binancePrice)
        # if self.bittrexPrice != 0.0:
        #     prices.append(self.bittrexPrice)
        if self.coinbasePrice != 0.0:
            prices.append(self.coinbasePrice)
        if len(prices) > 0:
            for price in prices:
                if price > self.priceData['currentPrice']:
                    self.nowTrendAllDown = False
                if price < self.priceData['currentPrice']:
                    self.nowTrendAllUp = False
            self.nowTrendAvg = sum(prices) / len(prices)
            self.nowTrendStd = np.std(prices)
            if self.nowTrendAvg > self.priceData['currentPrice']:
                self.nowTrendBetAvg = max(self.priceData['currentPrice'], (self.nowTrendAvg - self.nowTrendStd))
            else:
                self.nowTrendBetAvg = min(self.priceData['currentPrice'], (self.nowTrendAvg + self.nowTrendStd))
            self.nowTrendConsensus = False
            if self.nowTrendBetAvg != self.priceData['currentPrice']:
                self.nowTrendConsensus = True
            self.nowTrendVeryUp = False
            self.nowTrendVeryDown = False
            if self.nowTrendConsensus:
                if self.priceToPrediction(self.nowTrendBetAvg) > ( 1.1 * self.feeAsPercent ):
                    self.nowTrendVeryUp = True
                if self.priceToPrediction(self.nowTrendBetAvg) < 0 - ( 1.1 * self.feeAsPercent ):
                    self.nowTrendVeryDown = True

    def getPriceChangeSinceLast(self):
        self.priceChange = self.previousCurrentPrice - self.priceData['currentPrice']
        self.percentChange = self.priceChange * 100 / self.previousCurrentPrice
        self.previousCurrentPrice = self.priceData['currentPrice']


    def predictPrice(self):
        # print("Count: {0:d}".format(self.r.llen('avgSets')))
        # print(pickle.loads(self.r.rpop('avgSets')))
        oldSet = pickle.loads(self.r.lindex('avgSets', (self.r.llen('avgSets') - 1)))
        newSet = pickle.loads(self.r.lindex('avgSets', 0))
        while self.r.llen('avgSets') > 10:
            oldSet = pickle.loads(self.r.rpop('avgSets'))
            # print(oldSet)
            newSet = pickle.loads(self.r.lindex('avgSets', 0))
            # print(newSet)

        newSet = pickle.loads(self.r.lindex('avgSets', 0))
        newSet['currentPrice'] = self.priceData['currentPrice']

        result_list = sorted(newSet.keys(), key=lambda x: newSet[x], reverse=True)
        i = 1
        for key in result_list:
            if key == 'currentPrice':
                self.pricePosition = i
                break
            i = i + 1


        if self.r.llen('avgSets') == 10:
            self.tSecSlope = newSet['tSecAvg'] - oldSet['tSecAvg']
            self.fiveSlope = newSet['fiveAvg'] - oldSet['fiveAvg']
            self.thirtySlope = newSet['thirtyAvg'] - oldSet['thirtyAvg']
            self.oneTwentySlope = newSet['oneTwentyAvg'] - oldSet['oneTwentyAvg']
            self.predictionString = categories.getRollingString(self.priceData['currentPrice'], newSet['tSecAvg'], newSet['fiveAvg'], newSet['thirtyAvg'], newSet['oneTwentyAvg'])+'-'+categories.getSlopeString(self.tSecSlope, self.fiveSlope, self.thirtySlope, self.oneTwentySlope)
            cur.execute('SELECT tSecValue, fiveValue, thirtyValue, oneTwentyValue FROM predictions WHERE predictions.key = "{0:s}"'.format(self.predictionString))
            values = cur.fetchall()
            # self.predictionValue = 0
            # self.predictionLongValue = 0
            self.tSecPrediction = 0.0
            self.fivePrediction = 0.0
            self.thirtyPrediction = 0.0
            self.oneTwentyPrediction = 0.0
            for value in values:
                # self.predictionValue = value[0]
                # self.predictionLongValue = value[3]
                self.tSecPrediction = value[0]
                self.fivePrediction = value[1]
                self.thirtyPrediction = value[2]
                self.oneTwentyPrediction = value[3]
            now = datetime.now()
            tSecExpire = now + timedelta(seconds=30)
            fiveExpire = now + timedelta(minutes=5)
            thirtyExpire = now + timedelta(minutes=30)
            oneTwentyExpire = now + timedelta(minutes=120)


            while self.r.llen('tSecPredictionSet') > 1:
                self.r.rpop('tSecPredictionSet')
            if self.r.llen('tSecPredictionSet') == 1:
                self.tSecPredictionSet = pickle.loads(self.r.lindex('tSecPredictionSet', 0))
            if self.tSecPrediction != 0.0:
                self.tSecPredictionSet.append({
                    'value' : self.tSecPrediction,
                    'price' : self.predictionToPrice(self.tSecPrediction),
                    'expires' : tSecExpire
                })
            self.tSecPredictionSet = [ pSet for pSet in self.tSecPredictionSet if ( ( isUnderPrediction(pSet) and self.priceData['currentPrice'] > pSet['price'] ) or ( isOverPrediction(pSet) and self.priceData['currentPrice'] < pSet['price'] ) ) ]
            self.tSecPredictionSet = list(filter(isStillValid, self.tSecPredictionSet))
            self.r.lpush('tSecPredictionSet', pickle.dumps(self.tSecPredictionSet))
            tSecUnderSet = list(filter(isUnderPrediction, self.tSecPredictionSet))
            tSecOverSet = list(filter(isOverPrediction, self.tSecPredictionSet))
            self.predictionSpreads['tSec'] = {}
            self.predictionSpreads['tSec']['Over'] = self.calculateSpread(tSecOverSet)
            self.predictionSpreads['tSec']['Under'] = self.calculateSpread(tSecUnderSet)
            # self.tSecPredictionSpread = self.calculateSpread(self.tSecPredictionSet)


            while self.r.llen('fivePredictionSet') > 1:
                self.r.rpop('fivePredictionSet')
            if self.r.llen('fivePredictionSet') == 1:
                self.fivePredictionSet = pickle.loads(self.r.lindex('fivePredictionSet', 0))
            if self.fivePrediction != 0.0:
                self.fivePredictionSet.append({
                    'value' : self.fivePrediction,
                    'price' : self.predictionToPrice(self.fivePrediction),
                    'expires' : fiveExpire
                })

            self.fivePredictionSet = [ pSet for pSet in self.fivePredictionSet if ( ( isUnderPrediction(pSet) and self.priceData['currentPrice'] > pSet['price'] ) or ( isOverPrediction(pSet) and self.priceData['currentPrice'] < pSet['price'] ) ) ]

            self.fivePredictionSet = list(filter(isStillValid, self.fivePredictionSet))
            self.r.lpush('fivePredictionSet', pickle.dumps(self.fivePredictionSet))
            # self.fivePredictionSpread = self.calculateSpread(self.fivePredictionSet)

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



            while self.r.llen('thirtyPredictionSet') > 1:
                self.r.rpop('thirtyPredictionSet')
            if self.r.llen('thirtyPredictionSet') == 1:
                self.thirtyPredictionSet = pickle.loads(self.r.lindex('thirtyPredictionSet', 0))
            if self.thirtyPrediction != 0.0:
                self.thirtyPredictionSet.append({
                    'value' : self.thirtyPrediction,
                    'price' : self.predictionToPrice(self.thirtyPrediction),
                    'expires' : thirtyExpire
                })

            self.thirtyPredictionSet = [ pSet for pSet in self.thirtyPredictionSet if ( ( isUnderPrediction(pSet) and self.priceData['currentPrice'] > pSet['price'] ) or ( isOverPrediction(pSet) and self.priceData['currentPrice'] < pSet['price'] ) ) ]

            self.thirtyPredictionSet = list(filter(isStillValid, self.thirtyPredictionSet))
            self.r.lpush('thirtyPredictionSet', pickle.dumps(self.thirtyPredictionSet))
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
            # self.thirtyPredictionSpread = self.calculateSpread(self.thirtyPredictionSet)


            while self.r.llen('oneTwentyPredictionSet') > 1:
                self.r.rpop('oneTwentyPredictionSet')
            if self.r.llen('oneTwentyPredictionSet') == 1:
                self.oneTwentyPredictionSet = pickle.loads(self.r.lindex('oneTwentyPredictionSet', 0))
            if self.oneTwentyPrediction != 0.0:
                self.oneTwentyPredictionSet.append({
                    'value' : self.oneTwentyPrediction,
                    'price' : self.predictionToPrice(self.oneTwentyPrediction),
                    'expires' : oneTwentyExpire
                })
            self.oneTwentyPredictionSet = [ pSet for pSet in self.oneTwentyPredictionSet if ( ( isUnderPrediction(pSet) and self.priceData['currentPrice'] > pSet['price'] ) or ( isOverPrediction(pSet) and self.priceData['currentPrice'] < pSet['price'] ) ) ]

            self.oneTwentyPredictionSet = list(filter(isStillValid, self.oneTwentyPredictionSet))


            self.r.lpush('oneTwentyPredictionSet', pickle.dumps(self.oneTwentyPredictionSet))

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

            # self.sixtyPredictionSpread = self.calculateSpread(sixtySet)
            # self.ninetyPredictionSpread = self.calculateSpread(ninetySet)
            # self.oneTwentyPredictionSpread = self.calculateSpread(oneTwentySet)

    def getPossibleNextGreen(self, price):
        return self.feeCalc(price, self.fee, self.startPeakFeeMultiple, self.lastAction)

    def calculateSpread(self, tSet):
        spread = {
            'min' : self.priceData['currentPrice'],
            'mid' : self.priceData['currentPrice'],
            'avg' : self.priceData['currentPrice'],
            'max' : self.priceData['currentPrice'],
            'std' : 0.0,
            'betAvg' : self.priceData['currentPrice'],
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
            if spread['avg'] > self.priceData['currentPrice']:
                spread['betAvg'] = max(self.priceData['currentPrice'], (spread['avg'] - spread['std']))
            else:
                spread['betAvg'] = min(self.priceData['currentPrice'], (spread['avg'] + spread['std']))

        return spread

    def priceToPredictionWcolor(self, price):
        prediction = ( price / self.priceData['currentPrice'] * 100 ) - 100
        return self.colorizePrediction(prediction)

    def colorizePrediction(self, prediction):
        up = bcolors.OKGREEN
        down = bcolors.FAIL
        smallup = bcolors.OKBLUE
        smalldn = bcolors.WARNING
        if self.tryingToBuy():
            up = bcolors.FAIL
            down = bcolors.OKGREEN
            smallup = bcolors.WARNING
            smalldn = bcolors.OKBLUE
        if prediction == -100:
            return bcolors.ENDC + '{0:7.3f}'.format(0.0) + bcolors.ENDC
        if prediction == 0.000:
            return bcolors.ENDC + '{0:7.3f}'.format(0.0) + bcolors.ENDC
        if prediction > self.feeAsPercent:
            return up + '{0:7.3f}'.format(prediction) + bcolors.ENDC
        if prediction < 0 - self.feeAsPercent:
            return down + '{0:7.3f}'.format(prediction) + bcolors.ENDC
        if prediction > 0:
            return smallup + '{0:7.3f}'.format(prediction) + bcolors.ENDC
        if prediction < 0:
            return smalldn + '{0:7.3f}'.format(prediction) + bcolors.ENDC
        return bcolors.ENDC + '{0:7.3f}'.format(prediction) + bcolors.ENDC

    def priceToPrediction(self, price):
        prediction = ( price / self.priceData['currentPrice'] * 100 ) - 100
        if prediction == -100:
            return 0.0
        return prediction

    def predictionToPrice(self, prediction):
        return self.priceData['currentPrice'] * (( prediction + 100 ) / 100)

    def topCet(self):
        if self.cetBuyTime < datetime.now() - timedelta(minutes=10):
            get_account(self)
            self.getAccountValue()
            if self.available['CET'] < self.cetPile:
                if self.cetId != "0":
                    print(" Cancel existing cet order ")
                    cancel_order(self.cetId, self)
                if self.available[self.buyCoin] == 0.0 and self.available[self.sellCoin] == 0.0:
                    print("Looking for CET when no funds are available", flush=True)
                    exit()
                elif self.available[self.buyCoin] > self.available[self.sellCoin]:
                    if self.buyCoin not in ['BTC', 'USDT', 'ETH', 'BCH']:
                        return
                    market = 'CET'+self.buyCoin
                else:
                    if self.sellCoin not in ['BTC', 'USDT', 'ETH', 'BCH']:
                        return
                    market = 'CET'+self.sellCoin

                cetData = get_latest(self, market, 1)

                data = {
                    "amount": 100,
                    "price": cetData["currentPrice"],
                    "type": "buy",
                    "market": market
                }
                print("Buying CET")
                self.cetId = put_limit(data, 'cet')
                self.cetBuyTime = datetime.now()
                get_account(self)
                self.getAccountValue()




    def calculateLean(self, mainData):
        self.USDTlean=1.0
        if self.sellCoin != 'USDT' and self.buyCoin != 'USDT':
            sellUSDData = get_latest(self, self.sellCoin + 'USDT', 1)
            self.currentPrice[self.sellCoin] = sellUSDData['currentPrice']
            buyUSDData = get_latest(self, self.buyCoin + 'USDT', 1)
            self.currentPrice[self.buyCoin] = buyUSDData['currentPrice']
            self.USDTlean = ( buyUSDData['currentPrice'] / sellUSDData['currentPrice'] ) / mainData['currentPrice']
        if self.USDTlean < 0.0000001:
            self.USDTlean = 1.0
        usdtPrice = self.priceData['currentPrice'] * self.USDTlean

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
        self.btcPrice = self.priceData['currentPrice'] * self.BTClean

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
        self.ethPrice = self.priceData['currentPrice'] * self.ETHlean

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
        self.bchPrice = self.priceData['currentPrice'] * self.BCHlean

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

        sellTime = 1
        buyTime = 1
        timeRange = timedelta(hours = 48);
        now = datetime.now() - timeRange
        lowValX = (1.0 / float(timeRange.total_seconds()))
        highValX = (float(timeRange.total_seconds()) / 1.0)
        self.historyLean=1.0
        skipCount = len(self.history) - 4
        skipCounter = 0
        if len(self.history) != 0:
            for trade in self.history[::-1]:
                # print("time: "+str(trade['finished_time']), flush=True)
                if skipCounter > skipCount:
                    since = datetime.fromtimestamp(trade['finished_time']) - now
                    s1 = since.total_seconds()
                    m = (s1 / 60)
                    if trade['type'] == 'buy':
                        # print("buyTime: "+str(m)+' '+str(trade['finished_time']), flush=True)
                        buyTime += m
                    else:
                        sellTime += m
                        # print("sellTime: "+str(m)+' '+str(trade['finished_time']), flush=True)
                    now = datetime.fromtimestamp(trade['finished_time'])
                skipCounter += 1
            since = datetime.now() - now
            s1 = since.total_seconds()
            m = (s1 / 60)
            if self.nextActionType() == 'buy':
                buyTime += m
                # print("buyTime: "+str(m)+' to now', flush=True)
            else:
                sellTime += m
                # print("sellTime: "+str(m)+' to now', flush=True)

            maxLean=1.5
            minLean=0.5
            lowValX=0.000347342827371
            unscaledLean = buyTime / sellTime
            historyLean = ( (math.log(unscaledLean - (lowValX / 2 * unscaledLean) ) * 0.000019) + lowValX ) / lowValX
            if self.nextActionType() == 'buy':
                if historyLean > 1.0:
                    self.historyLean = 1 - ( historyLean - 1 )
                else:
                    self.historyLean = ( 1 - historyLean ) + 1
            else:
                if historyLean > 1.0:
                    self.historyLean = ( 1 - historyLean ) + 1
                else:
                    self.historyLean = 1 - ( historyLean - 1 )

        cTo = float(mainData['highPrice']) - float(mainData['currentPrice'])
        if self.nextActionType() == 'sell':
            cTo = float(mainData['currentPrice']) - float(mainData['lowPrice'])
        lowHighDiff = float(mainData['highPrice']) - float(mainData['lowPrice'])


        unscaledNum = self.goalPercentReset - (cTo * self.goalPercentReset / lowHighDiff)
        self.recentRangeLean = scale(unscaledNum, 0.85, 1.25, 0.01, self.goalPercentReset)

        since = datetime.now() - self.lastTime
        seconds = since.total_seconds()
        self.timeLean = 2.26 - ((math.sqrt(seconds) - (0.006 * seconds)) / 25)
        if seconds > 5000:
            self.timeLean = 0.85

        greenTouchCount = self.greenTouches
        if self.redTouches > 0:
            greenTouchCount += 1
        self.greenTouchesLean = 1.2 - (0.2 * greenTouchCount)
        if self.greenTouchesLean < 0.01:
            self.greenTouchesLean = 0.01



    def getGoalPercent(self):

        goal = self.goalPercentReset
        # goal = goal * self.timeLean
        # goal = goal * self.currencyLean
        # goal = goal * self.historyLean
        # goal = goal * self.recentRangeLean
        # goal = goal * self.greenTouchesLean
        # if goal < 1.00001:
        #     goal = 1.00001
        return goal



    def checkOrderPending(self):
        result = order_pending(self.market)

        if self.lastAction == 'reset' or self.lastAction == 'hardReset':
            if result['id'] != "0":
                cancel_order(result['id'], self)
                self.tradeId = 0
                result['orderUp'] = False
                self.topCet()
            if self.lastAction == 'reset':
                self.history = order_finished(self.market, 1, 100, self)
            if self.lastAction == 'reset' or self.lastAction == 'hardReset':
                get_account(self)
                self.getAccountValue()
                if self.available[self.buyCoin] == 0.0 and self.available[self.sellCoin] == 0.0:
                    print("Looking for new price when no funds are available", flush=True)
                    exit()
                elif self.sellCoin == 'USDT' and self.available[self.sellCoin] < 10.0:
                    self.lastAction = 'buy'
                elif self.available[self.buyCoin] > self.available[self.sellCoin]:
                    self.lastAction = 'buy'
                else:
                    self.lastAction = 'sell'
                self.pickNewLastPrice()
        else:
            if result['orderUp'] == False:
                self.history = order_finished(self.market, 1, 100, self)
                try:
                    self.cetSellHistory = order_finished('CET'+self.sellCoin, 1, 100, self)
                except KeyError:
                    self.cetSellHistory = {}
                except AttributeError:
                    self.cetSellHistory = {}
                try:
                    self.cetBuyHistory = order_finished('CET'+self.buyCoin, 1, 100, self)
                except KeyError:
                    self.cetBuyHistory = {}
                except AttributeError:
                    self.cetBuyHistory = {}
                self.goalPercent = self.getGoalPercent()


        if result['orderUp'] == False:
            if self.lastPrice == 0.0:
                print("No previous price to set new price", flush=True)
                self.pickNewLastPrice()
            self.mode = 'wait'
            self.tradeId = "0"
            # get price
            self.setGreenPrice()
            self.setInitalGuidePrice()
            self.updateGuidePrice(self.guidePrice)
        else:
            self.tradeId=result['id']
            if float(result['amount']) != 0.0:
                self.amount=result['amount']
                self.remaining=result['remaining']

        if result['remaining'] == result['amount']:
            return False

        now = datetime.now()

        if now - self.listedTime > timedelta(minutes=5):
            print(" Trade in progress for more tha 5 min, canceling now")
            cancel_order(self.tradeId, self)
            self.tradeId = 0
            return False

        print(bcolors.OKBLUE)
        print(" ")
        print("Trade in progress amount: "+result['amount']+" remaining: "+result['remaining'])
        print(now.astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p"))
        print(bcolors.ENDC)
        print(" ")
        print(now.strftime("%Y-%m-%d %H:%M:%S"), flush=True)

        return True

    def tryingToBuy(self):
        if self.lastAction == 'sell':
            return True
        return False

    def tryingToSell(self):
        if self.lastAction == 'buy':
            return True
        return False

    def pickNewLastPrice(self):
        if self.tryingToSell():
            priceRange = float(self.priceData['highPrice']) - float(self.priceData['lowPrice'])
            newLastPrice = float(self.priceData['lowPrice']) + ( ( priceRange * ( self.lowStartPercent * 100 ) ) / 100 )
            if newLastPrice < self.lastPrice or self.lastPrice == 0:
                self.lastPrice = newLastPrice
        else:
            priceRange = float(self.priceData['highPrice']) - float(self.priceData['lowPrice'])
            newLastPrice = float(self.priceData['lowPrice']) + ( ( priceRange * ( self.highStartPercent * 100 ) ) / 100)
            if newLastPrice > self.lastPrice or self.lastPrice == 0:
                self.lastPrice = newLastPrice
        # print('<><><><><><>pick new last price: '+str(self.lastPrice))

    def fiveMinPredictionIsGreen(self):
        if self.tryingToBuy():
            if self.avgFiveMinPrediction() < self.greenPrice:
                return True
        else:
            if self.avgFiveMinPrediction() > self.greenPrice:
                return True
        return False

    def twoMinPredictionIsGreen(self):
        if self.tryingToBuy():
            if self.avgTwoMinPrediction() < self.greenPrice:
                return True
        else:
            if self.avgTwoMinPrediction() > self.greenPrice:
                return True
        return False


    def avgFiveMinPrediction(self):
        if self.tryingToBuy():
            return (
                self.predictionSpreads['zeroOne']['Under']['betAvg'] +
                self.predictionSpreads['oneTwo']['Under']['betAvg']
            ) / 2
        else:
            return (
                self.predictionSpreads['zeroOne']['Over']['betAvg'] +
                self.predictionSpreads['oneTwo']['Over']['betAvg']
            ) / 2

    def avgTwoMinPrediction(self):
        if self.tryingToBuy():
            return (
                self.predictionSpreads['threeFour']['Under']['betAvg'] +
                self.predictionSpreads['fourFive']['Under']['betAvg']
            ) / 2
        else:
            return (
                self.predictionSpreads['threeFour']['Over']['betAvg'] +
                self.predictionSpreads['fourFive']['Over']['betAvg']
            ) / 2

    def bestFiveMinPrediction(self):
        if self.tryingToBuy():
            return min(
                self.predictionSpreads['zeroOne']['Under']['betAvg'],
                self.predictionSpreads['oneTwo']['Under']['betAvg']
            )
        else:
            return max(
                self.predictionSpreads['zeroOne']['Over']['betAvg'],
                self.predictionSpreads['oneTwo']['Over']['betAvg']
            )

    def bestTwoMinPrediction(self):
        if self.tryingToBuy():
            return min(
                self.predictionSpreads['threeFour']['Under']['betAvg'],
                self.predictionSpreads['fourFive']['Under']['betAvg']
            )
        else:
            return max(
                self.predictionSpreads['threeFour']['Over']['betAvg'],
                self.predictionSpreads['fourFive']['Over']['betAvg']
            )

    def fiveMinPredictionIsNextGreen(self, price):
        if self.tryingToBuy():
            if self.longTermTrendPrediction() > 0:
                if self.bestFiveMinForNextPrediction() > self.getPossibleNextGreen(price):
                    return True
            elif self.longTermTrendPrediction() < 0:
                if self.avgFiveMinForNextPrediction() > self.getPossibleNextGreen(price):
                    return True
        else:
            if self.longTermTrendPrediction() < 0:
                if self.bestFiveMinForNextPrediction() < self.getPossibleNextGreen(price):
                    return True
            elif self.longTermTrendPrediction() > 0:
                if self.avgFiveMinForNextPrediction() < self.getPossibleNextGreen(price):
                    return True
        return False

    def fifteenMinPredictionIsNextGreen(self, price):
        if self.tryingToBuy():
            if self.longTermTrendPrediction() > 0:
                if self.bestFifteenMinForNextPrediction() > self.getPossibleNextGreen(price):
                    return True
            elif self.longTermTrendPrediction() < 0:
                if self.avgFifteenMinForNextPrediction() > self.getPossibleNextGreen(price):
                    return True
        else:
            if self.longTermTrendPrediction() < 0:
                if self.bestFifteenMinForNextPrediction() < self.getPossibleNextGreen(price):
                    return True
            elif self.longTermTrendPrediction() > 0:
                if self.avgFifteenMinForNextPrediction() < self.getPossibleNextGreen(price):
                    return True
        return False

    def avgFiveMinForNextPrediction(self):
        if self.tryingToBuy():
            return (
                self.predictionSpreads['twoThree']['Over']['betAvg'] +
                self.predictionSpreads['oneTwo']['Over']['betAvg'] +
                self.predictionSpreads['zeroOne']['Over']['betAvg'] ) / 3
        else:
            return (
                self.predictionSpreads['twoThree']['Under']['betAvg'] +
                self.predictionSpreads['oneTwo']['Under']['betAvg'] +
                self.predictionSpreads['zeroOne']['Under']['betAvg'] ) / 3

    def bestFiveMinForNextPrediction(self):
        if self.tryingToBuy():
            return min(
                self.predictionSpreads['twoThree']['Over']['betAvg'],
                self.predictionSpreads['oneTwo']['Over']['betAvg'],
                self.predictionSpreads['zeroOne']['Over']['betAvg'])
        else:
            return max(
                self.predictionSpreads['twoThree']['Under']['betAvg'],
                self.predictionSpreads['oneTwo']['Under']['betAvg'],
                self.predictionSpreads['zeroOne']['Under']['betAvg'])

    def avgFifteenMinForNextPrediction(self):
        if self.tryingToBuy():
            return self.predictionSpreads['fifteenThirty']['Over']['betAvg']
        else:
            return self.predictionSpreads['fifteenThirty']['Under']['betAvg']

    def bestFifteenMinForNextPrediction(self):
        if self.tryingToBuy():
            return self.predictionSpreads['fifteenThirty']['Over']['betAvg']
        else:
            return self.predictionSpreads['fifteenThirty']['Under']['betAvg']

    def thirtySecondsPredictionIsGreen(self):
        if self.tryingToBuy():
            if self.bestThirtySecondsPrediction() < self.greenPrice:
                return True
        else:
            if self.bestThirtySecondsPrediction() > self.greenPrice:
                return True
        return False

    def bestThirtySecondsPrediction(self):
        if self.tryingToBuy():
            return min(self.predictionToPrice(self.tSecPrediction), self.predictionSpreads['tSec']['Under']['betAvg'])
        else:
            return max(self.predictionToPrice(self.tSecPrediction), self.predictionSpreads['tSec']['Over']['betAvg'])

    def twoMinPredictionIsNextGreen(self, price):
        if self.tryingToBuy():
            if self.longTermTrendPrediction() > 0:
                if self.bestTwoMinForNextPrediction() > self.getPossibleNextGreen(price):
                    return True
            elif self.longTermTrendPrediction() < 0:
                if self.avgTwoMinForNextPrediction() > self.getPossibleNextGreen(price):
                    return True
        else:
            if self.longTermTrendPrediction() < 0:
                if self.bestTwoMinForNextPrediction() < self.getPossibleNextGreen(price):
                    return True
            elif self.longTermTrendPrediction() > 0:
                if self.avgTwoMinForNextPrediction() < self.getPossibleNextGreen(price):
                    return True
        return False

    def avgTwoMinForNextPrediction(self):
        if self.tryingToBuy():
            return (
                self.predictionSpreads['fourFive']['Over']['betAvg'] +
                self.predictionSpreads['threeFour']['Over']['betAvg'] +
                self.predictionSpreads['twoThree']['Over']['betAvg'] ) / 3
        else:
            return (
                self.predictionSpreads['fourFive']['Under']['betAvg'] +
                self.predictionSpreads['threeFour']['Under']['betAvg'] +
                self.predictionSpreads['twoThree']['Under']['betAvg'] ) / 3

    def bestTwoMinForNextPrediction(self):
        if self.tryingToBuy():
            return min(
                self.predictionSpreads['fourFive']['Over']['betAvg'],
                self.predictionSpreads['threeFour']['Over']['betAvg'],
                self.predictionSpreads['twoThree']['Over']['betAvg'])
        else:
            return max(
                self.predictionSpreads['fourFive']['Under']['betAvg'],
                self.predictionSpreads['threeFour']['Under']['betAvg'],
                self.predictionSpreads['twoThree']['Under']['betAvg'])

    def isLongTermTrendVeryAwayFromGreen(self):
        if self.tryingToBuy():
            if self.longTermTrendPrice() < self.greenPrice:
                return False
            percent = (self.longTermTrendPrice() * 100 / self.greenPrice ) - 100
            # print("PERCENT DEBUG: {0:0.2f}".format(percent), flush=True)
            if percent > ( 3.5 * self.feeAsPercent ):
                return True
            return False
        else:
            if self.longTermTrendPrice() > self.greenPrice:
                return False
            percent = (self.longTermTrendPrice() * 100 / self.greenPrice ) - 100
            # print("PERCENT DEBUG: {0:0.2f}".format(percent), flush=True)
            if percent < 0 - ( 3.5 * self.feeAsPercent ):
                return True
            return False

    def isLongTermTrendVeryUp(self):
        if self.longTermTrendPrediction() > ( 3 * self.feeAsPercent ):
            return True
        return False

    def isLongTermTrendVeryDown(self):
        if self.longTermTrendPrediction() < 0 - ( 3 * self.feeAsPercent ):
            return True
        return False

    def longTermTrendPrediction(self):
        longPrice = self.longTermTrendPrice()
        return self.priceToPrediction(longPrice)

    def longTermTrendPrice(self):
        if 'sixtyNinety' not in self.predictionSpreads:
            self.predictPrice()
        longBetterAverages = [
            self.predictionSpreads['ninetyOneTwenty']['Over']['betAvg'],
            self.predictionSpreads['sixtyNinety']['Over']['betAvg'],
            # self.predictionSpreads['thirtySixty']['Over']['betAvg'],
            # self.predictionSpreads['zeroThirty']['Over']['betAvg'],
            self.predictionSpreads['ninetyOneTwenty']['Under']['betAvg'],
            self.predictionSpreads['sixtyNinety']['Under']['betAvg'],
            # self.predictionSpreads['thirtySixty']['Under']['betAvg'],
            # self.predictionSpreads['zeroThirty']['Under']['betAvg']
        ]
        totals = []
        for betAvg in longBetterAverages:
            if betAvg != self.priceData['currentPrice']:
                totals.append(betAvg)
        if len(totals) == 0:
            return self.priceData['currentPrice']
        return sum(totals) / len(totals)

    def isPriceGoodForExpiringTrade(self, price, seconds, name):
        if self.tryingToBuy():
            if ( price < self.greenPrice
                and price < self.priceData['currentPrice'] ):
                price, name = self.checkLongTermTrendOnPrice(price, name)
                self.setExpiringTrade({
                    'name' : name,
                    'price': price,
                    'expires': datetime.now() + timedelta(seconds=seconds)
                })
        if self.tryingToSell():
            if ( price > self.greenPrice
                and price > self.priceData['currentPrice'] ):
                price, name = self.checkLongTermTrendOnPrice(price, name)
                self.setExpiringTrade({
                    'name' : name,
                    'price': price,
                    'expires': datetime.now() + timedelta(seconds=seconds)
                })

    def checkLongTermTrendOnPrice(self, price, name):
        startPrice = price
        if self.tryingToBuy():
            if self.priceData['currentPrice'] < price:
                price = self.priceData['currentPrice']
                name = name + '-curentPrice'
            if self.isLongTermTrendVeryDown() and not self.nowTrendStrongUp():
                if self.BCHlean < 1:
                    price = price * self.BCHlean
                    name = name + '-BCHleanLongTermTrend=' + str(self.BCHlean)
                if self.ETHlean < 1:
                    price = price * self.ETHlean
                    name = name + '-ETHleanLongTermTrend=' + str(self.ETHlean)
                if self.USDTlean < 1:
                    price = price * self.USDTlean
                    name = name + '-USDTleanLongTermTrend=' + str(self.USDTlean)
                if self.BTClean < 1:
                    price = price * self.BTClean
                    name = name + '-BTCleanLongTermTrend=' + str(self.BTCTlean)
            if self.allBetterEstimatesDown() and not self.nowTrendStrongUp():
                if self.BCHlean < 1:
                    price = price * self.BCHlean
                    name = name + '-BCHleanAllBetterEst=' + str(self.BCHlean)
                if self.ETHlean < 1:
                    price = price * self.ETHlean
                    name = name + '-ETHleanAllBetterEst=' + str(self.ETHlean)
                if self.USDTlean < 1:
                    price = price * self.USDTlean
                    name = name + '-USDTleanAllBetterEst=' + str(self.USDTlean)
                if self.BTClean < 1:
                    price = price * self.BTClean
                    name = name + '-BTCleanAllBetterEst=' + str(self.BTCTlean)
            if self.nowTrendMildDown():
                if self.BCHlean < 1:
                    price = price * self.BCHlean
                    name = name + '-BCHleanNowTrendMild=' + str(self.BCHlean)
                if self.ETHlean < 1:
                    price = price * self.ETHlean
                    name = name + '-ETHleanNowTrendMild=' + str(self.ETHlean)
                if self.USDTlean < 1:
                    price = price * self.USDTlean
                    name = name + '-USDTleanNowTrendMild=' + str(self.USDTlean)
                if self.BTClean < 1:
                    price = price * self.BTClean
                    name = name + '-BTCleanNowTrendMild=' + str(self.BTCTlean)
            if self.nowTrendStrongDown():
                if self.BCHlean < 1:
                    price = price * self.BCHlean
                    name = name + '-BCHleanNowTrend=' + str(self.BCHlean)
                if self.ETHlean < 1:
                    price = price * self.ETHlean
                    name = name + '-ETHleanNowTrend=' + str(self.ETHlean)
                if self.USDTlean < 1:
                    price = price * self.USDTlean
                    name = name + '-USDTleanNowTrend=' + str(self.USDTlean)
                if self.BTClean < 1:
                    price = price * self.BTClean
                    name = name + '-BTCleanNowTrend=' + str(self.BTCTlean)
            if self.pricePosition < 3:
                price = price * 0.9999
                name = name + '-pricePosition'
            if self.thirtySlope < 0:
                price = price + ( self.thirtySlope / 2 )
                name = name + '-thirtySlope'
            if self.oneTwentySlope < 0:
                price = price + ( self.oneTwentySlope * 2 )
                name = name + '-oneTwentySlope'

            print("Price lowered {0:0.2f} from {1:0.2f} to {2:0.2f}".format((startPrice - price), startPrice, price), flush=True)
            return price, name

        else:
            if self.priceData['currentPrice'] > price:
                    price = self.priceData['currentPrice']
            if self.isLongTermTrendVeryUp() and not self.nowTrendStrongDown():
                if self.BCHlean > 1:
                    price = price * self.BCHlean
                    name = name + '-BCHleanLongTermTrend=' + str(self.BCHlean)
                if self.ETHlean > 1:
                    price = price * self.ETHlean
                    name = name + '-ETHleanLongTermTrend=' + str(self.ETHlean)
                if self.USDTlean > 1:
                    price = price * self.USDTlean
                    name = name + '-USDTleanLongTermTrend=' + str(self.USDTlean)
                if self.BTClean > 1:
                    price = price * self.BTClean
                    name = name + '-BTCleanLongTermTrend=' + str(self.BTCTlean)
            if self.allBetterEstimatesUp() and not self.nowTrendStrongDown():
                if self.BCHlean > 1:
                    price = price * self.BCHlean
                    name = name + '-BCHleanAllBetterEst=' + str(self.BCHlean)
                if self.ETHlean > 1:
                    price = price * self.ETHlean
                    name = name + '-ETHleanAllBetterEst=' + str(self.ETHlean)
                if self.USDTlean > 1:
                    price = price * self.USDTlean
                    name = name + '-USDTleanAllBetterEst=' + str(self.USDTlean)
                if self.BTClean > 1:
                    price = price * self.BTClean
                    name = name + '-BTCleanAllBetterEst=' + str(self.BTCTlean)
            if self.nowTrendMildUp():
                if self.BCHlean > 1:
                    price = price * self.BCHlean
                    name = name + '-BCHleanNowTrendMild=' + str(self.BCHlean)
                if self.ETHlean > 1:
                    price = price * self.ETHlean
                    name = name + '-ETHleanNowTrendMild=' + str(self.ETHlean)
                if self.USDTlean > 1:
                    price = price * self.USDTlean
                    name = name + '-USDTleanNowTrendMild=' + str(self.USDTlean)
                if self.BTClean > 1:
                    price = price * self.BTClean
                    name = name + '-BTCleanNowTrendMild=' + str(self.BTCTlean)
            if self.nowTrendStrongUp():
                if self.BCHlean > 1:
                    price = price * self.BCHlean
                    name = name + '-BCHleanNowTrend=' + str(self.BCHlean)
                if self.ETHlean > 1:
                    price = price * self.ETHlean
                    name = name + '-ETHleanNowTrend=' + str(self.ETHlean)
                if self.USDTlean > 1:
                    price = price * self.USDTlean
                    name = name + '-USDTleanNowTrend=' + str(self.USDTlean)
                if self.BTClean > 1:
                    price = price * self.BTClean
                    name = name + '-BTCleanNowTrend=' + str(self.BTCTlean)
            if self.pricePosition > 3:
                price = price * 1.0001
                name = name + '-pricePosition'
            if self.thirtySlope > 0:
                price = price + ( self.thirtySlope / 2 )
                name = name + '-thirtySlope'
            if self.oneTwentySlope > 0:
                price = price + ( self.oneTwentySlope * 2 )
                name = name + '-oneTwentySlope'
            print("Price raised {0:0.2f} from {1:0.2f} to {2:0.2f}".format((price - startPrice), startPrice, price), flush=True)
            return price, name

    def allBetterEstimatesUp(self):
        for group in self.predictionSpreads:
            for direction in self.predictionSpreads[group]:
                if self.predictionSpreads[group][direction]['betAvg'] < self.priceData['currentPrice']:
                    return False
        return True

    def allBetterEstimatesDown(self):
        for group in self.predictionSpreads:
            for direction in self.predictionSpreads[group]:
                if self.predictionSpreads[group][direction]['betAvg'] > self.priceData['currentPrice']:
                    return False
        return True

    def getNiceActionPrice(self):
        if self.tryingToBuy():
            return self.priceData['currentPrice'] * 0.999999
        return self.priceData['currentPrice'] * 1.000001

    def nowTrendMildUp(self):
        if self.nowTrendVeryUp:
            return True
        if self.nowTrendStrongUp():
            return True
        if self.priceToPrediction(self.nowTrendAvg) > self.feeAsPercent:
            return True
        return False

    def nowTrendStrongUp(self):
        if self.nowTrendVeryUp:
            return True
        if ( self.nowTrendAllUp and self.nowTrendConsensus
            and self.priceToPrediction(self.nowTrendAvg) > self.feeAsPercent ):
            return True
        return False

    def nowTrendMildDown(self):
        if self.nowTrendVeryDown:
            return True
        if self.nowTrendStrongDown():
            return True
        if self.priceToPrediction(self.nowTrendAvg) < 0 - self.feeAsPercent:
            return True
        return False

    def nowTrendStrongDown(self):
        if self.nowTrendVeryDown:
            return True
        if ( self.nowTrendAllDown and self.nowTrendConsensus
            and self.priceToPrediction(self.nowTrendAvg) < 0 - self.feeAsPercent ):
            return True
        return False

    def checkGreenPrice(self):
        if self.greenPrice <= 0:
            self.setGreenPrice()
        if self.greenPrice <= 0:
            print("LAST PRICE ERROR!",flush=True)
            print("LAST PRICE ERROR!",flush=True)
            print("LAST PRICE ERROR!",flush=True)
            print("LAST PRICE ERROR!",flush=True)
            self.pickNewLastPrice()
            self.setGreenPrice()

    def setGreenPrice(self):
        self.greenPrice = self.feeCalc(self.lastPrice, self.fee, self.startPeakFeeMultiple, self.nextActionType())

    def addCurrentPrice(self, priceData):

        self.checkGreenPrice()

        self.priceData=priceData
        modeWas = self.mode
        if (self.tryingToSell() and
                self.priceData['currentPrice'] > self.greenPrice):
            self.mode = 'green'
        elif (self.tryingToSell() and
                self.priceData['currentPrice'] < self.greenPrice):
            if self.mode != 'red':
                self.mode = 'wait'
        elif (self.tryingToBuy() and
                self.priceData['currentPrice'] > self.greenPrice):
            if self.mode != 'red':
                self.mode = 'wait'
        elif (self.tryingToBuy() and
                self.priceData['currentPrice'] < self.greenPrice):
            self.mode = 'green'

        # cTo = self.priceData['highPrice'] - self.priceData['currentPrice']
        # if self.nextActionType() == 'sell':
        #     cTo = self.priceData['currentPrice'] - self.priceData['lowPrice']
        # lowHighDiff = self.priceData['highPrice'] - self.priceData['lowPrice']

        # currentRangePerc = ( cTo * 100 / lowHighDiff )
        # # print('currentRangePerc {0:0.8f}'.format(currentRangePerc))

        # if ( datetime.now() - self.lastTime > timedelta(hours=self.redTimeHours()) and
        #         self.mode != 'green'):
        #     self.mode = 'red'
        #     self.redTouches += 1
        #     lossGainPerc = ( self.lastPrice * 100 / self.priceData['currentPrice'] ) - 100
        #     if self.nextActionType() == 'sell':
        #         lossGainPerc = ( self.priceData['currentPrice'] * 100 / self.lastPrice ) - 100

        #     cTo = self.priceData['highPrice'] - self.priceData['currentPrice']
        #     if self.nextActionType() == 'sell':
        #         cTo = self.priceData['currentPrice'] - self.priceData['lowPrice']
        #     lowHighDiff = self.priceData['highPrice'] - self.priceData['lowPrice']

        #     currentRangePerc = ( cTo * 100 / lowHighDiff )
        #     # print('lossGainPerc {0:5.2f} currentRangePerc {1:5.2f}'.format(lossGainPerc, currentRangePerc), flush=True)
        #     if lossGainPerc < -1.0 and ( lossGainPerc > -1.5 or self.tryingToBuy() ) and currentRangePerc > 95.0:
        #         print("RED TRADE - DISABLED")
                # self.setRedTrade()

            # Rewrite red mode to look for a new good entry point and make a red sale
            #  return to the "never move green price" maxium
            #
            # Also set to never take a > 1.2% loss
            #
            # Also make the trigger a % from green after time X
            #
            # if self.tryingToSell():
            #     self.greenPrice = self.greenPrice * 0.999995
            #     if datetime.now() - self.lastTime > timedelta(hours=self.resetHardTimeHours):
            #         self.greenPrice = self.greenPrice * 0.99995
            # else:
            #     self.greenPrice = self.greenPrice * 1.000005
            #     if datetime.now() - self.lastTime > timedelta(hours=self.resetHardTimeHours):
            #         self.greenPrice = self.greenPrice * 1.00005
            # self.setInitalGuidePrice()
            # self.updateGuidePrice(self.guidePrice)

        if modeWas != 'green' and self.mode == 'green':
            self.greenTouches += 1

        if ( ( datetime.now() - self.actionTime > timedelta(minutes=1)
                and self.listedType == 'action' )
            or ( datetime.now() - self.goalTime > timedelta(minutes=10)
                and self.listedType == 'goal' ) ):
            # reboot guide price replacing action trade witha goal trade
            self.setInitalGuidePrice()
            self.updateGuidePrice(self.guidePrice)

        self.goalPercent = self.getGoalPercent()

        if self.mode == 'green':
            if self.isLongTermTrendVeryAwayFromGreen():
                if self.tryingToBuy() and not self.nowTrendStrongDown():
                    self.actionPrice = self.getNiceActionPrice()
                    self.setActionTrade('buyPredictions')
                    return
                if self.tryingToSell() and not self.nowTrendStrongUp():
                    self.actionPrice = self.getNiceActionPrice()
                    self.setActionTrade('sellPredictions')
                    return
            if self.tryingToBuy() and self.nowTrendVeryUp:
                self.actionPrice = self.getNiceActionPrice()
                self.setActionTrade('greenTrendVeryUp')
                return
            if self.tryingToSell() and self.nowTrendVeryDown:
                self.actionPrice = self.getNiceActionPrice()
                self.setActionTrade('greenTrendVeryDown')
                return
        else:
            if self.actionPrice != self.greenPrice:
                self.setInitalGuidePrice()
                self.updateGuidePrice(self.guidePrice)

            if self.tryingToBuy() and self.nowTrendVeryUp:
                self.setRedTrade('redTrendVeryUp')
                return
            if self.tryingToSell() and self.nowTrendVeryDown:
                self.setRedTrade('redTrendVeryDown')
                return
            if self.tryingToBuy():
                if self.longTermTrendPrediction() > 0:
                    if ( self.fiveMinPredictionIsNextGreen(self.priceData['currentPrice'])
                            and self.twoMinPredictionIsNextGreen(self.priceData['currentPrice']) ):
                        self.setRedTrade('redPredictionsBuy')
                        return
                if self.isLongTermTrendVeryAwayFromGreen() or self.allBetterEstimatesUp():
                    if ( self.twoMinPredictionIsNextGreen(self.priceData['currentPrice'])
                            and self.fiveMinPredictionIsNextGreen(self.priceData['currentPrice'])):
                        self.setRedTrade('redPredictionsBuyLoose')
                        return
            if self.tryingToSell():
                if self.longTermTrendPrediction() < 0:
                    if ( self.fiveMinPredictionIsNextGreen(self.priceData['currentPrice'])
                        and self.twoMinPredictionIsNextGreen(self.priceData['currentPrice']) ):
                        self.setRedTrade('redPredictionsSell')
                        return


        # if self.r.llen('expiringTrades') > 0:
            # self.isPriceGoodForExpiringTrade(self.tSecPrediction, 60)
            # self.isPriceGoodForExpiringTrade(self.fivePrediction, 300)
        self.isPriceGoodForExpiringTrade(self.ethPrice, 23, 'ETH')
        self.isPriceGoodForExpiringTrade(self.bchPrice, 23, 'BCH')
        self.isPriceGoodForExpiringTrade(self.coinbasePrice, 23, 'coinbase')
        self.isPriceGoodForExpiringTrade(self.binancePrice, 23, 'binance')
        self.isPriceGoodForExpiringTrade(self.huobiPrice, 23, 'huobi')
        self.isPriceGoodForExpiringTrade(self.bittrexPrice, 23, 'bittrex')
        self.isPriceGoodForExpiringTrade(self.nowTrendBetAvg, 39, 'nowTrendBetAvg')


        # thirty second data is too janky to use
        #
        # if self.thirtySecondsPredictionIsGreen():
        #     # print("CHECKING IN GREEN BUY PREDICTION", flush=True)
        #     # We should be able to buy within 30 seconds.
        #     price = self.checkLongTermTrendOnPrice(self.bestThirtySecondsPrediction())

        #     if ( self.twoMinPredictionIsNextGreen(price)
        #             or self.fiveMinPredictionIsNextGreen(self.priceData['currentPrice'])):
        #         print('30 Sec is IN Now Green, 5 min is IN next Green', flush=True)
        #         print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), flush=True)
        #         self.setExpiringTrade({
        #             'price': price,
        #             'expires': datetime.now() + timedelta(seconds=60)
        #         })

        if self.twoMinPredictionIsGreen():
            name = '2minGreen5minNext'
            price, name = self.checkLongTermTrendOnPrice(self.bestTwoMinPrediction(), name)
            if self.fiveMinPredictionIsNextGreen(self.priceData['currentPrice']):
                print('2 min is IN Now Green, 5 min is IN next Green', flush=True)
                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), flush=True)
                self.setExpiringTrade({
                    'name': name,
                    'price': price,
                    'expires': datetime.now() + timedelta(seconds=45)
                })

        if self.fiveMinPredictionIsGreen():
            name = '5minGreen15minNext'
            price, name = self.checkLongTermTrendOnPrice(self.bestFiveMinPrediction(), name)
            if self.fifteenMinPredictionIsNextGreen(self.priceData['currentPrice']):
                print('5 min is IN Now Green, 15 min is IN next Green', flush=True)
                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), flush=True)
                self.setExpiringTrade({
                    'name': name,
                    'price': price,
                    'expires': datetime.now() + timedelta(seconds=75)
                })



        # elif self.tryingToSell():
        #     if self.thirtySecondsPredictionIsGreen():
        #         # print("CHECKING IN GREEN BUY PREDICTION", flush=True)
        #         # We should be able to sell within 30 seconds.
        #         price = self.checkLongTermTrendOnPrice(self.bestThirtySecondsPrediction())
        #         if ( self.twoMinPredictionIsNextGreen(price)
        #                 or self.fiveMinPredictionIsNextGreen(self.priceData['currentPrice'])):
        #             print('30 Sec is IN Now Green, 5 min is IN next Green', flush=True)
        #             print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), flush=True)
        #             self.setExpiringTrade({
        #                 'price': price,
        #                 'expires': datetime.now() + timedelta(seconds=60)
        #             })
        #     if self.twoMinPredictionIsGreen():
        #         price = self.checkLongTermTrendOnPrice(self.bestTwoMinPrediction())
        #         if self.fiveMinPredictionIsNextGreen(self.priceData['currentPrice']):
        #             self.setExpiringTrade({
        #                 'price': price,
        #                 'expires': datetime.now() + timedelta(seconds=120)
        #             })


            # if ( ( self.priceToPrediction(self.predictionSpread['tSec']['Under']['betAvg']) > (2 * self.feeAsPercent) or self.priceToPrediction(self.predictionSpreads['five']['Over']['betAvg'])  > (2 * self.feeAsPercent) )
            #     and self.minLongPrediction > self.feeAsPercent):
            #     print("PRICE IS RIPE FOR A SELL, LEAVE THIS POTENTIAL AND BUY NOW TO CATCH IT", flush=True)
            #     if self.predictionSpreads['five']['Over']['betAvg'] > self.getPossibleNextGreen(price):
            #         self.setRedTrade()

        # if self.tryingToBuy() and self.priceData['currentPrice'] < self.greenPrice:
        #     # print("CHECKING IN GREEN BUY PREDICTION", flush=True)
        #     if ( ( self.priceToPrediction(self.tSecPredictionSpread['betAvg']) > self.feeAsPercent or self.priceToPrediction(self.fivePredictionSpread['betAvg']) > self.feeAsPercent )
        #         and self.avgLongPrediction > 0):
        #         # print("IN GREEN TRIGGER BUY", flush=True)
        #         self.actionPrice = self.priceData['currentPrice'] - (self.feePriceUnit(self.priceData['currentPrice'], self.fee) * .001)
        #         self.setActionTrade()
        #         return
        #     else:
        #         self.updateGuidePrice(self.priceData['currentPrice'])
        # elif self.tryingToBuy():
        #     if ( ( self.priceToPrediction(self.tSecPredictionSpread['betAvg'])  > (2 * self.feeAsPercent) or self.priceToPrediction(self.fivePredictionSpread['betAvg'])  > (2 * self.feeAsPercent) )
        #         and self.minLongPrediction > self.feeAsPercent):
        #         print("PRICE IS RIPE FOR A SELL, LEAVE THIS POTENTIAL AND BUY NOW TO CATCH IT", flush=True)
        #         self.setRedTrade()
        #     # else:
        #     #     self.updateGuidePrice(self.priceData['currentPrice'])
        # elif self.tryingToSell() and self.priceData['currentPrice'] > self.greenPrice:
        #     # print("CHECKING IN GREEN SELL PREDICTION", flush=True)
        #     if ( ( self.priceToPrediction(self.tSecPredictionSpread['betAvg']) < 0 - ( 2 * self.feeAsPercent ) or self.priceToPrediction(self.fivePredictionSpread['betAvg']) < 0 - ( 2 * self.feeAsPercent ) )
        #         and self.avgLongPrediction < 0 - ( 2 * self.feeAsPercent )):
        #         # print("IN GREEN TRIGGER SELL", flush=True)
        #         self.actionPrice = self.priceData['currentPrice'] + (self.feePriceUnit(self.priceData['currentPrice'], self.fee) * .001)
        #         self.setActionTrade()
        #         return
        #     else:
        #         self.updateGuidePrice(self.priceData['currentPrice'])
        # elif self.tryingToSell() and self.avgPrediction < -3.75:
        #         # We don't red sell

        if self.tryingToSell() and self.priceData['currentPrice'] * 0.999 > self.greenPrice:
            self.latestPrice = self.priceData['sellActionPrice']
            if self.latestPrice > self.guidePrice:
                self.updateGuidePrice(self.latestPrice)
            elif self.mode == 'green' and self.priceData['buyActionPrice'] < self.actionPrice:
                self.actionPrice = self.priceData['currentPrice'] + (self.feePriceUnit(self.priceData['currentPrice'], self.fee) * 0.1)
                self.setActionTrade('classicActionSell')
        elif self.tryingToBuy() and self.priceData['currentPrice'] * 1.001 < self.greenPrice:
            self.latestPrice = self.priceData['buyActionPrice']
            if self.latestPrice < self.guidePrice:
                self.updateGuidePrice(self.latestPrice)
            elif self.mode == 'green' and self.priceData['sellActionPrice'] > self.actionPrice:
                self.actionPrice = self.priceData['currentPrice'] - (self.feePriceUnit(self.priceData['currentPrice'], self.fee) * 0.1)
                self.setActionTrade('classicActionBuy')


        if self.actionPrice == 0.0:
            self.actionPrice = self.greenPrice

        if self.r.llen('expiringTrades') > 0:
            self.checkExpiringTrades()

    def setInitalGuidePrice(self):
        self.guidePrice = self.greenPrice
        if self.latestPrice == 0.0:
            if self.tryingToBuy():
                self.latestPrice = self.priceData['buyActionPrice']
            else:
                self.latestPrice = self.priceData['sellActionPrice']
        if self.tryingToBuy():
            if self.guidePrice > self.latestPrice:
                self.guidePrice = self.latestPrice
            # self.guidePrice = self.guidePrice * self.historyLean
            # self.guidePrice = self.guidePrice * self.currencyLean
            if self.guidePrice > self.greenPrice:
                self.guidePrice = self.greenPrice
        else:
            if self.guidePrice < self.latestPrice:
                self.guidePrice = self.latestPrice
            # self.guidePrice = self.guidePrice * self.historyLean
            # self.guidePrice = self.guidePrice * self.currencyLean
            if self.guidePrice < self.greenPrice:
                self.guidePrice = self.greenPrice


    def updateGuidePrice(self, price):
        self.guidePrice=price
        if self.tryingToSell():
            print("self:"+str(self.actionPrice))
            actionChange=self.guidePrice - self.greenPrice
            goalChange=self.guidePrice - self.lastPrice
            actionPrice=self.greenPrice + ((actionChange * ( self.actionPercent * 100 ) ) / 100 )
            # print("real calc:"+str(actionPrice))
            if self.guidePrice * 0.99 > actionPrice:
                actionPrice = self.guidePrice * 0.99
                # print("min set:"+str(actionPrice))
            minAction = self.greenPrice
            if minAction > actionPrice:
                actionPrice = minAction
                # print("max set:"+str(actionPrice))
            if actionPrice > self.actionPrice:
                self.actionPrice = actionPrice
                # print("real set:"+str(actionPrice))
            self.goalPrice=self.greenPrice + ((goalChange * ( self.goalPercent * 100 ) ) / 100 )
            if self.goalPrice < 0:
                self.goalPrice = self.greenPrice * 1.5
            # print("last self:"+str(self.actionPrice), flush=True)
            self.replaceGoalTrade()
        elif self.tryingToBuy():
            print("self:"+str(self.actionPrice))
            actionChange=self.greenPrice - self.guidePrice
            goalChange=self.lastPrice - self.guidePrice
            actionPrice=self.greenPrice - ((actionChange * ( self.actionPercent * 100 ) ) / 100 )
            # print("real calc:"+str(actionPrice))
            if self.guidePrice * 1.01 < actionPrice:
                actionPrice = self.guidePrice * 1.01
                # print("min set:"+str(actionPrice))
            maxAction = self.greenPrice
            if maxAction < actionPrice:
                actionPrice = maxAction
                # print("max set:"+str(actionPrice))
            if actionPrice < self.actionPrice:
                # print("real set:"+str(actionPrice))
                self.actionPrice = actionPrice
            self.goalPrice=self.greenPrice - ((goalChange * ( self.goalPercent * 100 ) ) / 100 )
            if self.goalPrice < 0:
                self.goalPrice = self.greenPrice * 0.5
            # print("last self:"+str(self.actionPrice), flush=True)
            self.replaceGoalTrade()

    def replaceGoalTrade(self):
        if self.r.llen('expiringTrades') == 0:
            if self.tradeId != "0":
                print(" Cancel existing {0} order {1} for new GOAL ".format(self.actionTaken, self.tradeId))
                cancel_order(self.tradeId, self)
                self.tradeId = 0
                self.topCet()
            self.listedPrice = self.finalPriceSet(self.goalPrice)
            data = {
                "amount": self.nextAmount(self.listedPrice),
                "price": self.listedPrice,
                "type": self.nextActionType(),
                "market": self.market
            }
            self.listedTime = datetime.now()
            self.goalTime = datetime.now()
            self.listedType = 'goal'
            self.actionTaken='goal'
            self.actionValue=self.listedPrice
            print("Goal trade")
            newId=put_limit(data, self.listedType)
            self.tradeId=newId
            get_account(self)
            self.getAccountValue()

    def redTimeHours(self):
        if self.tryingToBuy():
            return 20
        return 40

    def setRedTrade(self, extra):
        now = datetime.now()
        lastRedTrade = now.timestamp()
        while self.r.llen('lastRedTrade') > 0:
            lastRedTrade = pickle.loads(self.r.rpop('lastRedTrade'))
        okTime = now - timedelta(hours=self.redTimeHours())
        print('RED TRADE DEBUG')
        print('lastRedTrade {0}'.format(lastRedTrade.astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p")))
        print('now {0}'.format(okTime.astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p")))

        redLoss = min(0, (self.endDealMoney - findDealMoney(self.priceData['currentPrice'], self.endAmount)))
        if self.tryingToSell():
            redLoss = min(0, (findDealMoney(self.priceData['currentPrice'], self.endAmount) - self.endDealMoney))
        if ( self.r.llen('expiringTrades') == 0
            and ( lastRedTrade < okTime
                or ( self.twelveHourProfit > 0
                    and redLoss + ( self.twelveHourProfit / 2 ) > 0 )
                or self.tryingToBuy() ) ):
            print(' ')
            print(' ')
            print(' ')
            print(' ')
            print(' ')
            print(' RED TRADE ')
            print(' ')
            print(' ')
            print(' ')
            print(' ')
            print(' ', flush=True)
            # return
            if self.tradeId != "0":
                print(" Cancel existing {0} order {1} for new RED ".format(self.actionTaken, self.tradeId))
                cancel_order(self.tradeId, self)
                self.tradeId = 0
            self.listedPrice = self.priceData['currentPrice']
            data = {
                "amount": self.nextAmount(self.listedPrice),
                "price": self.listedPrice,
                "type": self.nextActionType(),
                "market": self.market
            }
            self.listedTime = now
            self.actionTime = now
            self.listedType = 'red'
            self.actionTaken='red'
            self.r.lpush('lastRedTrade', pickle.dumps(now))
            self.actionValue=self.listedPrice
            print("RED Action trade")
            newId=put_limit(data, self.listedType, extra)
            self.tradeId=newId
            get_account(self)
            self.getAccountValue()
        else:
            print("DOUBLE RED ATTEMPT: DENIED", flush=True)

    def setExpiringTrade(self, info):
        if info['price'] > 0:
            self.r.lpush('expiringTrades', pickle.dumps({
                'tradeId': 0,
                'tradeType' : self.nextActionType(),
                'price': info['price'],
                'expires': info['expires'],
                'name': info['name']
            }))

    def checkExpiringTrades(self):
        bestTrade = None
        someTrades = False
        bestPrice = self.greenPrice
        length = self.r.llen('expiringTrades')
        for i in range(0, length):
            try:
                someTrades = True
                trade = pickle.loads(self.r.lindex('expiringTrades', i))
                priceBeyondGoal = False
                if self.tryingToBuy():
                    if trade['price'] < self.goalPrice:
                        priceBeyondGoal = True
                if self.tryingToSell():
                    if trade['price'] > self.goalPrice:
                        priceBeyondGoal = True
                if ( trade['expires'] < datetime.now() or
                        self.nextActionType() != trade['tradeType']
                        or trade['price'] <= 0.0
                        or priceBeyondGoal ):
                    if self.tradeId == trade['tradeId']:
                        print(" Cancel existing {0} order {1} for new EXPIRING check ".format(self.actionTaken, self.tradeId))
                        cancel_order(self.tradeId, self)
                        self.tradeId = 0
                    self.r.lrem('expiringTrades', 1, self.r.lindex('expiringTrades', i))
                else:
                    if self.tryingToBuy():
                        if trade['price'] < bestPrice:
                            bestPrice = trade['price']
                            bestTrade = trade
                    else:
                        if trade['price'] > bestPrice:
                            bestPrice = trade['price']
                            bestTrade = trade
            except TypeError:
                print("Pickle Problems")

        fiveMinAgo = datetime.now() - timedelta(minutes=2)
        oneMinAgo = datetime.now() - timedelta(seconds=45)
        tradeIsExpirable = False
        if self.listedType == 'action' or self.listedType == 'red':
            if self.listedTime < fiveMinAgo:
                tradeIsExpirable = True
            if self.listedTime < oneMinAgo:
                if self.tryingToSell():
                    if self.priceData['currentPrice'] > self.listedPrice:
                        tradeIsExpirable = True
                else:
                    if self.priceData['currentPrice'] < self.listedPrice:
                        tradeIsExpirable = True
        else:
            tradeIsExpirable = True

        if tradeIsExpirable:
            if bestTrade is not None:
                if self.listedPrice != bestTrade['price']:
                    if self.tradeId != "0":
                        print(" Cancel existing {0} order {1} for new EXPIRING best ".format(self.actionTaken, self.tradeId))
                        cancel_order(self.tradeId, self)
                        self.tradeId = 0
                    name = bestTrade['name']
                    price, name = self.checkLongTermTrendOnPrice(bestTrade['price'], name+'+')
                    self.listedPrice = self.finalPriceSet(price)
                    data = {
                        "amount": self.nextAmount(self.listedPrice),
                        "price": self.listedPrice,
                        "type": self.nextActionType(),
                        "market": self.market
                    }
                    self.listedTime = datetime.now()
                    self.listedType = 'expiring'
                    self.actionTaken = 'expiring'
                    self.actionValue = self.listedPrice
                    print("Expiring trade")
                    newId = put_limit(data, self.listedType, name)
                    self.tradeId=newId
                    get_account(self)
                    self.getAccountValue()
                    for i in range(0, self.r.llen('expiringTrades')):
                        try:
                            trade = pickle.loads(self.r.lindex('expiringTrades', i))
                            if trade == bestTrade:
                                self.r.lrem('expiringTrades', 1, pickle.dumps(trade))
                                bestTrade['tradeId'] = newId
                                self.r.lpush('expiringTrades', pickle.dumps(bestTrade))
                        except TypeError:
                            print("Pickle Problems")
            elif someTrades:
                self.replaceGoalTrade()


    def setSlopeTrade(self):
        if self.tradeId != "0":
            print(" Cancel existing order for SLOPE ")
            cancel_order(self.tradeId, self)
            self.tradeId = 0
        if self.tryingToBuy():
            self.listedPrice = self.priceData['currentPrice'] * 1.000001
        if self.tryingToSell():
            self.listedPrice = self.priceData['currentPrice'] * 0.999999
        data = {
            "amount": self.nextAmount(self.listedPrice),
            "price": self.listedPrice,
            "type": self.nextActionType(),
            "market": self.market
        }
        self.listedTime = datetime.now()
        self.listedType = 'slope'
        self.actionTaken='slope'
        self.actionValue=self.listedPrice
        print("Slope trade")
        newId=put_limit(data, self.listedType)
        self.tradeId=newId
        get_account(self)
        self.getAccountValue()


    def setActionTrade(self, extra):
        if self.tradeId != "0":
            print(" Cancel existing {0} order {1} for new ACTION ".format(self.actionTaken, self.tradeId))
            cancel_order(self.tradeId, self)
            self.tradeId = 0
        self.listedPrice = self.finalPriceSet(self.actionPrice)
        data = {
            "amount": self.nextAmount(self.listedPrice),
            "price": self.listedPrice,
            "type": self.nextActionType(),
            "market": self.market
        }
        self.listedTime = datetime.now()
        self.listedType = 'action'
        self.actionTaken='action'
        self.actionValue=self.listedPrice
        print("Action trade")
        newId=put_limit(data, self.listedType, extra)
        self.tradeId=newId
        get_account(self)
        self.getAccountValue()

    def finalPriceSet(self, price):
        if self.tryingToBuy():
            if self.greenPrice < price:
                price = self.greenPrice
            if self.priceData['currentPrice'] < price:
                price = self.priceData['currentPrice'] * 0.999
        if self.tryingToSell():
            if self.greenPrice > price:
                price = self.greenPrice
            if self.priceData['currentPrice'] > price:
                price = self.priceData['currentPrice'] * 1.001
        return price

    def feeCalc(self, base, fee, multiple, action):
        baseFee = self.feePriceUnit(base, fee)
        finalFee = baseFee * multiple
        if action == 'sell':
            # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>set sell high "+str(base + finalFee)+" from base "+str(base))
            return base + finalFee
        else:
            # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>set buy low "+str(base - finalFee)+" from base "+str(base))
            return base - finalFee

    def feePriceUnit(self, price, fee): # 1 fee value at given price
        firstFee = price * fee
        secondFee = firstFee * fee
        thirdFee = secondFee * fee
        fourthFee = thirdFee * fee
        fifthFee = fourthFee * fee
        sixthFee = fifthFee * fee
        seventhFee = sixthFee * fee
        eightFee = seventhFee * fee
        return firstFee + secondFee + thirdFee + fourthFee + fifthFee + sixthFee + seventhFee + eightFee

    def nextActionType(self):
        if self.tryingToBuy():
            return 'buy'
        return 'sell'

    def nextAmount(self, price):
        get_account(self)
        self.getAccountValue()
        if self.tryingToBuy():
            return self.available[self.sellCoin] / price
        return self.available[self.buyCoin]

    def toUSDRaw(self, amount, coin):
        if coin == 'USDT':
            price = 1.0
        elif coin in self.currentPrice:
            price = self.currentPrice[coin]
        else:
            data = get_latest(self, coin + 'USDT', 1)
            price = data['currentPrice']
            self.currentPrice[coin] = price
        return amount * price

    def toUSD(self, amount, coin):
        return '${0:5.2f}'.format(self.toUSDRaw(amount, coin))


    def changeFormat(self, this, last, coin, toUsd = True):
        if last == 0.0:
            last = 0.0000001
        val = (this * 100 / last ) - 100
        currency = self.toUSD(abs(this - last),coin)
        if toUsd == False:
            change = abs(this - last)
            cf = getFloatFormat(change, 8)
            currency = currency + " {0:{1}.{2}f} {3:4s}".format(cf[0], cf[1], cf[2], coin)
        if round(val, 2) == 0.0:
            return bcolors.OKBLUE+'{0:5.2f}%'.format(0.0)+'      0'+bcolors.ENDC
        elif val > 0:
            return bcolors.OKGREEN+'{0:6.2f}%'.format(val)+' '+currency+bcolors.ENDC
        return bcolors.FAIL+'{0:6.2f}%'.format(val)+' '+currency+bcolors.ENDC


    def printDebug(self):
        print(" ")
        print(" ")

        buyAmount = self.available[self.buyCoin] + self.frozen[self.buyCoin]
        sellAmount = self.available[self.sellCoin] + self.frozen[self.sellCoin]

        sellTime = 1
        buyTime = 1
        buys=0
        sells=0
        buys24=0
        sells24=0
        buys12=0
        sells12=0
        total=0
        total24=0
        total12=0
        previous = 0.0
        previousDealMoney = 0.0
        previousDay = ''
        previousDealMoney = 0.0
        previousPrice=0.0
        startDealMoney = 0.0
        startAmount = 0.0
        startDealMoney24 = 0.0
        startAmount24 = 0.0
        startDealMoney12 = 0.0
        startAmount12 = 0.0
        self.endDealMoney = 0.0
        self.endAmount = 0.0
        previousAccountValue = 0.0
        accountValue = 0.0
        self.debugPrintRotation = 10
        now = datetime.now() - timedelta(hours = 48)
        if len(self.history) != 0:
            for trade in self.history[::-1]:
                if trade['status'] == 'done':
                    if startDealMoney == 0.0:
                        startDealMoney = float(trade['deal_money'])
                        startAmount = float(trade['amount'])
                    if datetime.fromtimestamp(trade['finished_time']) > datetime.now() - timedelta(hours = 24):
                        total24 += 1
                        if trade['type'] == 'buy':
                            buys24 += 1
                        else:
                            sells24 += 1
                        if startDealMoney24 == 0.0:
                            startDealMoney24 = float(trade['deal_money'])
                            startAmount24 = float(trade['amount'])
                    if datetime.fromtimestamp(trade['finished_time']) > datetime.now() - timedelta(hours = 12):
                        total12 += 1
                        if trade['type'] == 'buy':
                            buys12 += 1
                        else:
                            sells12 += 1
                        if startDealMoney12 == 0.0:
                            startDealMoney12 = float(trade['deal_money'])
                            startAmount12 = float(trade['amount'])

                    self.endDealMoney = float(trade['deal_money'])
                    self.endAmount = float(trade['amount'])
                    # print(complex_json.dumps(trade, indent = 4, sort_keys=True))
                    if self.debugPrintRotation > 5:
                        for cetSellTrade in self.cetSellHistory[::-1]:
                            if cetSellTrade['status'] == 'done':
                                if datetime.fromtimestamp(cetSellTrade['finished_time']) > now and datetime.fromtimestamp(cetSellTrade['finished_time']) < datetime.fromtimestamp(trade['finished_time']):
                                    print(datetime.fromtimestamp(cetSellTrade['finished_time']).astimezone(timezone('US/Central')).strftime("%I:%M%p")+' %4s CET %0.0f with %4s @ %s ' %(cetSellTrade['type'], float(cetSellTrade['amount']), self.sellCoin, self.toUSD(float(cetSellTrade['deal_money']), self.sellCoin)))
                        for cetBuyTrade in self.cetBuyHistory[::-1]:
                            if cetBuyTrade['status'] == 'done':
                                if datetime.fromtimestamp(cetBuyTrade['finished_time']) > now and datetime.fromtimestamp(cetBuyTrade['finished_time']) < datetime.fromtimestamp(trade['finished_time']):
                                    print(datetime.fromtimestamp(cetBuyTrade['finished_time']).astimezone(timezone('US/Central')).strftime("%I:%M%p")+' %4s CET %0.0f with %4s @ %s ' %(cetBuyTrade['type'], float(cetBuyTrade['amount']), self.buyCoin, self.toUSD(float(cetBuyTrade['deal_money']), self.buyCoin)))
                    makerTaker = ''
                    since = datetime.fromtimestamp(trade['finished_time']) - now
                    s1 = since.total_seconds()
                    m = (s1 / 60)
                    if trade['type'] == 'buy':
                        buyTime += m
                        buys += 1
                    else:
                        sellTime += m
                        sells += 1
                    total += 1
                    hours1, remainder = divmod(s1, 3600)
                    minutes1, seconds = divmod(remainder, 60)
                    now = datetime.fromtimestamp(trade['finished_time'])
                    day = datetime.fromtimestamp(trade['finished_time']).astimezone(timezone('US/Central')).strftime("%Y-%m-%d")
                    if self.debugPrintRotation > 5:
                        if day != previousDay:
                            print(day)
                    previousDay = day
                    hmstr = '{0:0.0f} m '.format(minutes1)
                    dealMoney = ''
                    if previousDealMoney != 0.0:
                        dealMoney = self.changeFormat(float(trade['deal_money']), previousDealMoney, self.sellCoin)
                    previousDealMoney = float(trade['deal_money'])
                    change = self.changeFormat(float(trade['amount']), previous, self.buyCoin)
                    if float(hours1) != 0.0:
                        hmstr = '{0:0.0f} hr'.format(hours1)
                    formatted1 = getFloatFormat(float(trade['avg_price']))
                    formatted2 = getFloatFormat(float(trade['amount']), 5)
                    if self.debugPrintRotation > 5:
                        if previousPrice != 0.0:
                            print('              '+percChange(float(trade['price']), previousPrice)+' tradeId: '+str(trade['id']), flush=True)
                        print(datetime.fromtimestamp(trade['finished_time']).astimezone(timezone('US/Central')).strftime("%I:%M%p")+' {0:5s} {1:4s} {2:{3}.{4}f} {5:3s}: {6:{7}.{8}f} {9:4s}: {10:7.7s} {11:15s} {12:15s}'.format(hmstr, trade['type'], formatted1[0], formatted1[1], formatted1[2], self.buyCoin, formatted2[0], formatted2[1], formatted2[2], self.sellCoin, self.toUSD(float(trade['deal_money']), self.sellCoin), dealMoney, change))
                    previous = float(trade['amount'])
                    previousPrice = float(trade['price'])
                    if trade['id'] in self.newHistory:
                        accountValue = self.newHistory[trade['id']]['accountValue']
                        if previousAccountValue != 0.0:
                            print('Account Value: ${0:0.2f} {1:s}'.format(accountValue, self.changeFormat(accountValue, previousAccountValue, 'USDT')))
                    previousAccountValue = accountValue
                # print('%2.0fhr %2.0fm %4s %0.8f ' %(float(hours1), float(minutes1), trade['type'], float(trade['avg_price'])))
        if len(self.cetSellHistory) > 0 and len(self.cetBuyHistory) > 0:
            for cetSellTrade in self.cetSellHistory[::-1]:
                if cetSellTrade['status'] == 'done':
                    if datetime.fromtimestamp(cetSellTrade['finished_time']) > now:
                        print(datetime.fromtimestamp(cetSellTrade['finished_time']).astimezone(timezone('US/Central')).strftime("%I:%M%p")+' %4s CET %0.0f with %4s @ %s ' %(cetSellTrade['type'], float(cetSellTrade['amount']), self.sellCoin, self.toUSD(float(cetSellTrade['deal_money']), self.sellCoin)))
            for cetBuyTrade in self.cetBuyHistory[::-1]:
                if cetBuyTrade['status'] == 'done':
                    if datetime.fromtimestamp(cetBuyTrade['finished_time']) > now:
                        print(datetime.fromtimestamp(cetBuyTrade['finished_time']).astimezone(timezone('US/Central')).strftime("%I:%M%p")+' %4s CET %0.0f with %4s @ %s ' %(cetBuyTrade['type'], float(cetBuyTrade['amount']), self.buyCoin, self.toUSD(float(cetBuyTrade['deal_money']), self.buyCoin)))
        if startDealMoney12 == 0.0:
            startDealMoney12 = startDealMoney24
        totalChangeDeal = self.changeFormat(self.endDealMoney, startDealMoney, self.sellCoin)
        totalChangeAmount = self.changeFormat(self.endAmount, startAmount, self.buyCoin, False)
        totalChangeDeal24 = self.changeFormat(self.endDealMoney, startDealMoney24, self.sellCoin)
        totalChangeAmount24 = self.changeFormat(self.endAmount, startAmount24, self.buyCoin, False)


        totalChangeDeal12 = self.changeFormat(self.endDealMoney, startDealMoney12, self.sellCoin)
        totalChangeAmount12 = self.changeFormat(self.endAmount, startAmount12, self.buyCoin, False)

        self.twelveHourProfit = self.endDealMoney - startDealMoney12
        # sdm = getFloatFormat(startDealMoney)
        # edm = getFloatFormat(self.endDealMoney)
        # sa = getFloatFormat(startAmount)
        # ea = getFloatFormat(self.endAmount)
        # print('dm start: {0:{1}.{2}f} dm end: {3:{4}.{5}f} a start: {6:{7}.{8}f} a end: {9:{10}.{11}f}'.format(sdm[0], sdm[1], sdm[2], edm[0], edm[1], edm[2], sa[0], sa[1], sa[2], ea[0], ea[1], ea[2]))
        if self.debugPrintRotation > 5:
            self.debugPrintRotation = 0
            print('              '+percChange(self.priceData['currentPrice'], previousPrice), flush=True)

        print("predictions:")
        print("p: {0:s}  ".format(self.predictionString))

        print("                    30 seconds {0:d}                        5 minutes {1:d}                          30 minutes {2:d}                    120 min tot {3:d}".format(
            len(self.tSecPredictionSet), len(self.fivePredictionSet), len(self.thirtyPredictionSet), len(self.oneTwentyPredictionSet)))
        print("current: {0:7s} {1:^28.28s}   {2:7s} {3:^28.28s}   {4:7s} {5:^28.28s}   {4:7s} {5:^28.28s}".format(
            self.colorizePrediction(self.tSecPrediction),
            self.getChangePredictionDebug(self.predictionToPrice(self.tSecPrediction), self.endAmount, self.endDealMoney),
            self.colorizePrediction(self.fivePrediction),
            self.getChangePredictionDebug(self.predictionToPrice(self.fivePrediction), self.endAmount, self.endDealMoney),
            self.colorizePrediction(self.thirtyPrediction),
            self.getChangePredictionDebug(self.predictionToPrice(self.thirtyPrediction), self.endAmount, self.endDealMoney),
            self.colorizePrediction(self.oneTwentyPrediction),
            self.getChangePredictionDebug(self.predictionToPrice(self.oneTwentyPrediction), self.endAmount, self.endDealMoney),
            ))
        # print(" ")
        # print(" ")
        # print("{0:>7.7s} {0:3s}   {1:<5s}{2:^28.28s}   {3:^28.28s}   {4:^28.28s}   {5:^28.28s}   {6:^28.28s} {7:s}".format(
        #     ' ',
        #     'count',
        #     'betAvg',
        #     'min',
        #     'mid',
        #     'avg',
        #     'max',
        #     'std'
        # ))
        # for group in self.predictionSpreads:
        #     for direction in self.predictionSpreads[group]:
        #         print("{0:>7.7s} {1:<5s} {13:3d}   {2:7s} {3:^28.28s}   {4:7s} {5:^28.28s}   {6:7s} {7:^28.28s}   {8:7s} {9:^28.28s}   {10:7s} {11:^28.28s} {12:7.3f}".format(
        #             group,
        #             direction,
        #             self.priceToPredictionWcolor(self.predictionSpreads[group][direction]['betAvg']),
        #             self.getChangePredictionDebug(self.predictionSpreads[group][direction]['betAvg'], self.endAmount, self.endDealMoney),
        #             self.priceToPredictionWcolor(self.predictionSpreads[group][direction]['min']),
        #             self.getChangePredictionDebug(self.predictionSpreads[group][direction]['min'], self.endAmount, self.endDealMoney),
        #             self.priceToPredictionWcolor(self.predictionSpreads[group][direction]['mid']),
        #             self.getChangePredictionDebug(self.predictionSpreads[group][direction]['mid'], self.endAmount, self.endDealMoney),
        #             self.priceToPredictionWcolor(self.predictionSpreads[group][direction]['avg']),
        #             self.getChangePredictionDebug(self.predictionSpreads[group][direction]['avg'], self.endAmount, self.endDealMoney),
        #             self.priceToPredictionWcolor(self.predictionSpreads[group][direction]['max']),
        #             self.getChangePredictionDebug(self.predictionSpreads[group][direction]['max'], self.endAmount, self.endDealMoney),
        #             self.predictionSpreads[group][direction]['std'],
        #             self.predictionSpreads[group][direction]['count']
        #         ))
        #     if group == 'five':
        #         print(" ")
        #     print(" ")

        print('in 48hr '+str(total)+' trades '+str(buys)+' buys '+str(sells)+' sells')
        print(' deal money: '+totalChangeDeal +' amount: '+totalChangeAmount)
        print('in 24hr '+str(total24)+' trades '+str(buys24)+' buys '+str(sells24)+' sells')
        print(' deal money: '+totalChangeDeal24 +' amount: '+totalChangeAmount24)
        print('in 12hr '+str(total12)+' trades '+str(buys12)+' buys '+str(sells12)+' sells')
        print(' deal money: '+totalChangeDeal12 +' amount: '+totalChangeAmount12)
        monthDeal = self.changeFormat(self.endDealMoney, 37.56, self.sellCoin)
        print('Summer reboot: '+monthDeal, flush=True)
        since = datetime.now() - now
        s1 = since.total_seconds()
        m = (s1 / 60)
        if self.nextActionType() == 'buy':
            buyTime += m
        else:
            sellTime += m
        hours1, remainder = divmod(s1, 3600)
        minutes1, seconds = divmod(remainder, 60)
        print(bcolors.OKBLUE+" >>> current <<<      %s: %0.4f %s: %0.4f CET: %0.0f"%(self.buyCoin, buyAmount, self.sellCoin, sellAmount, self.available['CET'])) # last succesful trade type
        print(bcolors.ENDC+"%2.0fhr %2.0fm %4s" %(float(hours1), float(minutes1), self.nextActionType()))

        now = datetime.now()
        # minutes = self.priceData['timeRange'].total_seconds() / 60
        # travel = ( self.priceData['highPrice'] - self.priceData['lowPrice'] ) / minutes
        # print(bcolors.ENDC+" time: {0:s} price gap pr min: {1:0.2f}".format(str(self.priceData['timeRange']),travel))
        # lowFormatted = getFloatFormat(self.priceData['lowPrice'])
        # highFormatted = getFloatFormat(self.priceData['highPrice'])
        # percentDiff = ( self.priceData['highPrice'] * 100 / self.priceData['lowPrice'] ) - 100
        # print("  low: {0:{1}.{2}f}   high: {3:{4}.{5}f}  diff: {6:0.2f}%".format(lowFormatted[0], lowFormatted[1], lowFormatted[2], highFormatted[0], highFormatted[1], highFormatted[2], percentDiff))
        # cToLow = getFloatFormat(self.priceData['currentPrice'] - self.priceData['lowPrice'])
        # cToHigh = getFloatFormat(self.priceData['highPrice'] - self.priceData['currentPrice'])
        # lowHighDiff = getFloatFormat(self.priceData['highPrice'] - self.priceData['lowPrice'])
        # print("c2low: {0:{1}.{2}f} c2high: {3:{4}.{5}f}  diff: {6:{7}.{8}f}".format(cToLow[0], cToHigh[1], cToLow[2], cToHigh[0], cToHigh[1], cToHigh[2], lowHighDiff[0], lowHighDiff[1], lowHighDiff[2]))
        # startFormatted = getFloatFormat(self.priceData['startPrice'])
        # endFormatted = getFloatFormat(self.priceData['currentPrice'])
        # if self.priceData['currentPrice'] < self.priceData['startPrice']:
        #     change = bcolors.FAIL+' down: {0:0.2f}'.format(self.priceData['startPrice'] - self.priceData['currentPrice'])
        # else:
        #     change = bcolors.OKGREEN+'   up: {0:0.2f}'.format(self.priceData['currentPrice'] - self.priceData['startPrice'])
        # print("start: {0:{1}.{2}f}    end: {3:{4}.{5}f} {6:s}{7:s}".format(startFormatted[0], startFormatted[1], startFormatted[2], endFormatted[0], endFormatted[1], endFormatted[2], change, bcolors.ENDC))
        # print(" current is {0:0.2f}% of recent range & {1:0.2f}% of green".format(cToLow[0] * 100 / lowHighDiff[0], self.percToGreen(self.priceData['currentPrice'])))
        try:
            print("current price fee ${0:0.2f}".format(self.feePriceUnit(self.priceData['currentPrice'], self.fee) * self.endAmount))
            # print("amount "+self.amount+" remaining "+self.remaining)

            # print("calc deal money ${0:0.2f}".format(findDealMoney(self.priceData['currentPrice'])))
            # print("previous deal money ${0:0.2f}".format(previousDealMoney))

            change = self.changeFormat(findDealMoney(self.priceData['currentPrice'], self.endAmount), self.endDealMoney, self.sellCoin)
            if self.nextActionType() == 'buy':
                change = self.changeFormat(findAmount(self.priceData['currentPrice'], self.endDealMoney), self.endAmount, self.buyCoin, False)
            color=''

            if self.mode == 'green':
                color=bcolors.OKGREEN
            if self.mode == 'in-progress':
                color=bcolors.OKBLUE
            if self.mode == 'red':
                color=bcolors.FAIL
            print(color+"   currentPrice: {0:0.8f} {1:s}".format(self.priceData['currentPrice'], change)+bcolors.ENDC)

            # print("calc deal money ${0:0.2f}".format(findDealMoney(self.greenPrice)))
            # print("previous deal money ${0:0.2f}".format(previousDealMoney))
            change = self.changeFormat(findDealMoney(self.greenPrice, self.endAmount), self.endDealMoney, self.sellCoin)
            if self.nextActionType() == 'buy':
                change = self.changeFormat(findAmount(self.greenPrice, self.endDealMoney), self.endAmount, self.buyCoin, False)

            print(bcolors.OKGREEN+"G R E E N Price: {0:0.8f} {1:s} {2:7.3f}".format(self.greenPrice, change, self.priceToPrediction(self.greenPrice))) # price most recently placed in a trade
            change = self.changeFormat(findDealMoney(self.listedPrice, self.endAmount), self.endDealMoney, self.sellCoin)
            if self.nextActionType() == 'buy':
                change = self.changeFormat(findAmount(self.listedPrice, self.endDealMoney), self.endAmount, self.buyCoin, False)
            print('{0:>15s}: {1:0.8f} {2:s}'.format("listed "+self.listedType, self.listedPrice, change), flush=True)

            if self.nextActionType() == 'sell':
                if self.actionPrice > self.greenPrice:
                    color=bcolors.OKGREEN
                else:
                    color=bcolors.WARNING
            else:
                if self.actionPrice < self.greenPrice:
                    color=bcolors.OKGREEN
                else:
                    color=bcolors.WARNING
            change = self.changeFormat(findDealMoney(self.actionPrice, self.endAmount), self.endDealMoney, self.sellCoin)
            if self.nextActionType() == 'buy':
                change = self.changeFormat(findAmount(self.actionPrice, self.endDealMoney), self.endAmount, self.buyCoin, False)
            print("{0:s}    actionPrice: {1:0.8f} {2:s}{3:s}".format(color, self.actionPrice, change, bcolors.ENDC)) # price we will sell at post hook
            change = self.changeFormat(findDealMoney(self.guidePrice, self.endAmount), self.endDealMoney, self.sellCoin)
            if self.nextActionType() == 'buy':
                change = self.changeFormat(findAmount(self.guidePrice, self.endDealMoney), self.endAmount, self.buyCoin, False)
            print("     guidePrice: {0:0.8f} {1:s}".format(self.guidePrice, change))
            change = self.changeFormat(findDealMoney(self.goalPrice, self.endAmount), self.endDealMoney, self.sellCoin)
            if self.nextActionType() == 'buy':
                change = self.changeFormat(findAmount(self.goalPrice, self.endDealMoney), self.endAmount, self.buyCoin, False)
            goalPercent = self.goalPercent * 100
            print("      goalPrice: {0:0.8f} {1:s} {2:0.2f}%".format(self.goalPrice, change, goalPercent))

            newSet = pickle.loads(self.r.lindex('avgSets', 0))
            newSet['currentPrice'] = self.priceData['currentPrice']

            result_list = sorted(newSet.keys(), key=lambda x: newSet[x], reverse=True)
            for key in result_list:
                slope = 0
                if key == 'tSecAvg':
                    slope = self.tSecSlope / 240
                elif key == 'fiveAvg':
                    slope = self.fiveSlope / 24
                elif key == 'thirtyAvg':
                    slope = self.thirtySlope / 4
                elif key == 'oneTwentyAvg':
                    slope = self.oneTwentySlope

                if self.nextActionType() == 'sell':
                    if slope > 0:
                        color=bcolors.OKGREEN
                    else:
                        color=bcolors.FAIL
                else:
                    if slope < 0:
                        color=bcolors.OKGREEN
                    else:
                        color=bcolors.FAIL

                if slope == 0:
                    slopeString = ""
                else:
                    slopeString = color+"{0:5.2f}".format(slope)+bcolors.ENDC

                print("  {0:10.10s}: {1:0.2f} {2:7s} slope {3}".format(key, newSet[key], self.priceToPredictionWcolor(newSet[key]), slopeString))


            print("  green touches: "+str(self.greenTouches))
            print("    red touches: "+str(self.redTouches))

            if self.isLongTermTrendVeryAwayFromGreen():
                print(boolColor(self.isLongTermTrendVeryAwayFromGreen(), False)+"isLongTermTrendVeryAwayFromGreen {0:b}".format(self.isLongTermTrendVeryAwayFromGreen())+bcolors.ENDC)

            if self.isLongTermTrendVeryUp():
                print(boolColor(self.isLongTermTrendVeryUp(), self.tryingToSell())+ "  isLongTermTrendVeryUp {0:b}".format(self.isLongTermTrendVeryUp())+bcolors.ENDC)
            if self.allBetterEstimatesUp():
                print(boolColor(self.allBetterEstimatesUp(), self.tryingToSell())+  "   allBetterEstimatesUp {0:b}".format(self.allBetterEstimatesUp())+bcolors.ENDC)
            if self.allBetterEstimatesDown():
                print(boolColor(self.allBetterEstimatesDown(), self.tryingToBuy())+ " allBetterEstimatesDown {0:b}".format(self.allBetterEstimatesDown())+bcolors.ENDC)
            if self.isLongTermTrendVeryDown():
                print(boolColor(self.isLongTermTrendVeryDown(), self.tryingToBuy())+"isLongTermTrendVeryDown {0:b}".format(self.isLongTermTrendVeryDown())+bcolors.ENDC)
            if self.fiveMinPredictionIsNextGreen(self.priceData['currentPrice']):
                print(boolColor(self.fiveMinPredictionIsNextGreen(self.priceData['currentPrice']))+"fiveMinPredictionIsNextGreen {0:b}".format(self.fiveMinPredictionIsNextGreen(self.priceData['currentPrice']))+bcolors.ENDC)
            if self.twoMinPredictionIsNextGreen(self.priceData['currentPrice']):
                print(boolColor(self.twoMinPredictionIsNextGreen(self.priceData['currentPrice']))+"twoMinPredictionIsNextGreen {0:b}".format(self.twoMinPredictionIsNextGreen(self.priceData['currentPrice']))+bcolors.ENDC)

            if self.nowTrendVeryUp:
                print(boolColor(self.nowTrendVeryUp, self.tryingToSell())+     "    nowTrendVeryUp {0:b}".format(self.nowTrendVeryUp)+bcolors.ENDC)
            if self.nowTrendStrongUp():
                print(boolColor(self.nowTrendStrongUp(), self.tryingToSell())+ "  nowTrendStrongUp {0:b}".format(self.nowTrendStrongUp())+bcolors.ENDC)
            if self.nowTrendAllUp:
                print(boolColor(self.nowTrendAllUp, self.tryingToSell())+      "     nowTrendAllUp {0:b}".format(self.nowTrendAllUp)+bcolors.ENDC)
            if self.nowTrendMildUp():
                print(boolColor(self.nowTrendMildUp(), self.tryingToSell())+   "    nowTrendMildUp {0:b}".format(self.nowTrendMildUp())+bcolors.ENDC)
            if self.nowTrendConsensus:
                print(boolColor(self.nowTrendConsensus)+                       " nowTrendConsensus {0:b}".format(self.nowTrendConsensus)+bcolors.ENDC)
            if self.nowTrendMildDown():
                print(boolColor(self.nowTrendMildDown(), self.tryingToBuy())+  "  nowTrendMildDown {0:b}".format(self.nowTrendMildDown())+bcolors.ENDC)
            if self.nowTrendAllDown:
                print(boolColor(self.nowTrendAllDown, self.tryingToBuy())+     "   nowTrendAllDown {0:b}".format(self.nowTrendAllDown)+bcolors.ENDC)
            if self.nowTrendStrongDown():
                print(boolColor(self.nowTrendStrongDown(), self.tryingToBuy())+"nowTrendStrongDown {0:b}".format(self.nowTrendStrongDown())+bcolors.ENDC)
            if self.nowTrendVeryDown:
                print(boolColor(self.nowTrendVeryDown, self.tryingToBuy())+    "  nowTrendVeryDown {0:b}".format(self.nowTrendVeryDown)+bcolors.ENDC)

            print("    longTermTrend: {0:0.2f} {1:s}".format(self.longTermTrendPrice(), self.colorizePrediction(self.longTermTrendPrediction())))
            if self.USDTlean != 1.0:
                print("  USDT -> {2:7s}: {0:0.2f} {1:7s}".format(self.usdtPrice, self.priceToPredictionWcolor(self.usdtPrice), self.market))
            if self.BTClean != 1.0:
                print("   BTC -> {2:7s}: {0:0.2f} {1:7s}".format(self.btcPrice, self.priceToPredictionWcolor(self.btcPrice), self.market))
            if self.BCHlean != 1.0:
                print("   BCH -> {2:7s}: {0:0.2f} {1:7s}".format(self.bchPrice, self.priceToPredictionWcolor(self.bchPrice), self.market))
            if self.ETHlean != 1.0:
                print("   ETH -> {2:7s}: {0:0.2f} {1:7s}".format(self.ethPrice, self.priceToPredictionWcolor(self.ethPrice), self.market))



            # print("coingecko BTCUSDT: {0:0.2f} {1:7s}".format(self.coingeckoPrice, self.priceToPredictionWcolor(self.coingeckoPrice)))
            print("  tetherPrice: ${0:2.6f}".format(self.tetherPrice), flush=True)
            print(" coinbase BTCUSDT: {0:0.2f} {1:7s}".format(self.coinbasePrice, self.priceToPredictionWcolor(self.coinbasePrice)))
            print("  binance BTCUSDT: {0:0.2f} {1:7s}".format(self.binancePrice, self.priceToPredictionWcolor(self.binancePrice)))
            print("    huobi BTCUSDT: {0:0.2f} {1:7s}".format(self.huobiPrice, self.priceToPredictionWcolor(self.huobiPrice)))
            print("* bittrex BTCUSDT: {0:0.2f} {1:7s}".format(self.bittrexPrice, self.priceToPredictionWcolor(self.bittrexPrice)))
            print("      nowTrendAvg: {0:0.2f} {1:7s}".format(self.nowTrendAvg, self.priceToPredictionWcolor(self.nowTrendAvg)))

            print("   nowTrendBetAvg: {0:0.2f} {1:7s}".format(self.nowTrendBetAvg, self.priceToPredictionWcolor(self.nowTrendBetAvg)))
            print("      nowTrendStd: {0:0.2f}".format(self.nowTrendStd))

            if self.priceChange < 0.0:
                print(bcolors.FAIL+"     price down: %0.8f" %abs(self.priceChange))
                print("     price down: %0.2f percent" %abs(self.percentChange)+bcolors.ENDC)
            elif self.priceChange > 0.0:
                print(bcolors.OKGREEN+"       price up: %0.8f" %abs(self.priceChange))
                print("       price up: %0.2f percent" %abs(self.percentChange)+bcolors.ENDC)
            # else:
            #     print("price didn't move")
            #     print("price didn't move")

            redLoss = min(0, (self.endDealMoney - findDealMoney(self.priceData['currentPrice'], self.endAmount)))
            if self.tryingToSell():
                redLoss = min(0, (findDealMoney(self.priceData['currentPrice'], self.endAmount) - self.endDealMoney))
            # print("redDebug: {0:f}".format((self.endDealMoney - findDealMoney(self.priceData['currentPrice'], self.endAmount))), flush=True)
            redEligible = False
            tColor = bcolors.FAIL
            if self.twelveHourProfit > 0 and (redLoss + ( self.twelveHourProfit / 2) ) > 0:
                redEligible = True
                tColor = bcolors.OKGREEN
            print(tColor+"twelveHourProfit {0:0.2f} redLoss {1:0.2f} redEligible {2:b}".format(self.twelveHourProfit, redLoss, redEligible)+bcolors.ENDC)

            if self.r.llen('lastRedTrade') > 0:
                tColor = ''
                lastRedTime = pickle.loads(self.r.lindex('lastRedTrade',0))
                if ( lastRedTime + timedelta(hours=self.redTimeHours()) ) < datetime.now():
                    tColor = bcolors.OKGREEN
                else:
                    tColor = bcolors.FAIL
                print(tColor+"Last Red Trade {0:s}".format(lastRedTime.astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p"))+bcolors.ENDC)
            else:
                print("No Last Red Trade Set")

            expiringTrades = False
            for i in range(0, self.r.llen('expiringTrades')):
                try:
                    expiringTrades = True
                    trade = pickle.loads(self.r.lindex('expiringTrades', i))
                    name = 'unk'
                    if 'name' in trade:
                        name = trade['name']
                    timeToExpire = trade['expires'] - datetime.now()
                    print("Price: {0:0.8f} type: {1:4s} expires: {2} source: {3:s}".format(trade['price'], trade['tradeType'], timeToExpire.seconds, name))
                except TypeError:
                    print("Pickle Problems")

            if not expiringTrades:
                print("No expiringTrades")

            # print("self.lastPrice {0} self.fee {1} self.startPeakFeeMultiple {2} self.nextActionType() {3}".format(self.lastPrice, self.fee, self.startPeakFeeMultiple, self.nextActionType()), flush=True)
            # print("self.priceData {0}".format(self.priceData))

                        # print(self.priceData['currentPrice'])
            # print("  30 sec slope {0:0.4f}".format(self.tSecSlope))
            # print("five min slope {0:0.4f}".format(self.fiveSlope ))
            # print("  30 min slope {0:0.4f}".format(self.thirtySlope))
            # print(" 120 min slope {0:0.4f}".format(self.oneTwentySlope))
            # print("five min trend {0}".format(self.fSlopeMode))
            # print("                       30 seconds {0:d}                           5 minutes {1:d}                          30 minutes {2:d}".format(
            #     len(self.tSecPredictionSet), len(self.fivePredictionSet), len(self.thirtyPredictionSet)))
            # print("current: {0:6.3f} {1:^28.28s}   {2:6.3f} {3:^28.28s}   {4:6.3f} {5:28s}".format(
            #     self.tSecPrediction,
            #     self.getChangePredictionDebug(self.predictionToPrice(self.tSecPrediction), self.endAmount, self.endDealMoney),
            #     self.fivePrediction,
            #     self.getChangePredictionDebug(self.predictionToPrice(self.fivePrediction), self.endAmount, self.endDealMoney),
            #     self.thirtyPrediction,
            #     self.getChangePredictionDebug(self.predictionToPrice(self.thirtyPrediction), self.endAmount, self.endDealMoney)
            #     ))
            # print("    min: {0:6.3f} {1:^28.28s}   {2:6.3f} {3:^28.28s}   {4:6.3f} {5:28s}".format(
            #     self.priceToPrediction(self.tSecPredictionSpread['min']),
            #     self.getChangePredictionDebug(self.tSecPredictionSpread['min'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.fivePredictionSpread['min']),
            #     self.getChangePredictionDebug(self.fivePredictionSpread['min'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.thirtyPredictionSpread['min']),
            #     self.getChangePredictionDebug(self.thirtyPredictionSpread['min'], self.endAmount, self.endDealMoney)
            #     ))
            # print("    mid: {0:6.3f} {1:^28.28s}   {2:6.3f} {3:^28.28s}   {4:6.3f} {5:28s}".format(
            #     self.priceToPrediction(self.tSecPredictionSpread['mid']),
            #     self.getChangePredictionDebug(self.tSecPredictionSpread['mid'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.fivePredictionSpread['mid']),
            #     self.getChangePredictionDebug(self.fivePredictionSpread['mid'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.thirtyPredictionSpread['mid']),
            #     self.getChangePredictionDebug(self.thirtyPredictionSpread['mid'], self.endAmount, self.endDealMoney)
            #     ))
            # print("    avg: {0:6.3f} {1:^28.28s}   {2:6.3f} {3:^28.28s}   {4:6.3f} {5:28s}".format(
            #     self.priceToPrediction(self.tSecPredictionSpread['avg']),
            #     self.getChangePredictionDebug(self.tSecPredictionSpread['avg'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.fivePredictionSpread['avg']),
            #     self.getChangePredictionDebug(self.fivePredictionSpread['avg'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.thirtyPredictionSpread['avg']),
            #     self.getChangePredictionDebug(self.thirtyPredictionSpread['avg'], self.endAmount, self.endDealMoney)
            #     ))
            # print("    max: {0:6.3f} {1:^28.28s}   {2:6.3f} {3:^28.28s}   {4:6.3f} {5:28s}".format(
            #     self.priceToPrediction(self.tSecPredictionSpread['max']),
            #     self.getChangePredictionDebug(self.tSecPredictionSpread['max'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.fivePredictionSpread['max']),
            #     self.getChangePredictionDebug(self.fivePredictionSpread['max'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.thirtyPredictionSpread['max']),
            #     self.getChangePredictionDebug(self.thirtyPredictionSpread['max'], self.endAmount, self.endDealMoney)
            #     ))
            # print("    std:     {0:10.3f}                            {1:10.3f}                            {2:10.3f}".format(
            #     self.tSecPredictionSpread['std'],
            #     self.fivePredictionSpread['std'],
            #     self.thirtyPredictionSpread['std']
            #     ))
            # print(" betAvg: {0:6.3f} {1:^28.28s}   {2:6.3f} {3:^28.28s}   {4:6.3f} {5:28s}".format(
            #     self.priceToPrediction(self.tSecPredictionSpread['betAvg']),
            #     "  N-A" if ( self.priceToPrediction(self.tSecPredictionSpread['betAvg']) == 0.0 ) else self.getChangePredictionDebug(self.tSecPredictionSpread['betAvg'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.fivePredictionSpread['betAvg']),
            #     "  N-A" if ( self.priceToPrediction(self.fivePredictionSpread['betAvg']) == 0.0 ) else self.getChangePredictionDebug(self.fivePredictionSpread['betAvg'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.thirtyPredictionSpread['betAvg']),
            #     "  N-A" if ( self.priceToPrediction(self.thirtyPredictionSpread['betAvg']) == 0.0 ) else self.getChangePredictionDebug(self.thirtyPredictionSpread['betAvg'], self.endAmount, self.endDealMoney)
            #     ))

            # print(" ")
            # print("120 min tot {3:d}      0-60 minutes {0:d}                        60-90 minutes {1:d}                      90-120 minutes {2:d}".format(
            #     self.sixtyPredictionCount, self.ninetyPredictionCount, self.oneTwentyPredictionCount, len(self.oneTwentyPredictionSet)))
            # print("current: {0:6.3f} {1:^28.28s}   {2:6.3f} {3:^28.28s}   {4:6.3f} {5:28s}".format(
            #     self.oneTwentyPrediction,
            #     self.getChangePredictionDebug(self.predictionToPrice(self.oneTwentyPrediction), self.endAmount, self.endDealMoney),
            #     0,
            #     '  N-A',
            #     0,
            #     '  N-A'
            #     ))
            # print("    min: {0:6.3f} {1:^28.28s}   {2:6.3f} {3:^28.28s}   {4:6.3f} {5:28s}".format(
            #     self.priceToPrediction(self.sixtyPredictionSpread['min']),
            #     self.getChangePredictionDebug(self.sixtyPredictionSpread['min'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.ninetyPredictionSpread['min']),
            #     self.getChangePredictionDebug(self.ninetyPredictionSpread['min'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.oneTwentyPredictionSpread['min']),
            #     self.getChangePredictionDebug(self.oneTwentyPredictionSpread['min'], self.endAmount, self.endDealMoney)
            #     ))
            # print("    mid: {0:6.3f} {1:^28.28s}   {2:6.3f} {3:^28.28s}   {4:6.3f} {5:28s}".format(
            #     self.priceToPrediction(self.sixtyPredictionSpread['mid']),
            #     self.getChangePredictionDebug(self.sixtyPredictionSpread['mid'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.ninetyPredictionSpread['mid']),
            #     self.getChangePredictionDebug(self.ninetyPredictionSpread['mid'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.oneTwentyPredictionSpread['mid']),
            #     self.getChangePredictionDebug(self.oneTwentyPredictionSpread['mid'], self.endAmount, self.endDealMoney)
            #     ))
            # print("    avg: {0:6.3f} {1:^28.28s}   {2:6.3f} {3:^28.28s}   {4:6.3f} {5:28s}".format(
            #     self.priceToPrediction(self.sixtyPredictionSpread['avg']),
            #     self.getChangePredictionDebug(self.sixtyPredictionSpread['avg'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.ninetyPredictionSpread['avg']),
            #     self.getChangePredictionDebug(self.ninetyPredictionSpread['avg'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.oneTwentyPredictionSpread['avg']),
            #     self.getChangePredictionDebug(self.oneTwentyPredictionSpread['avg'], self.endAmount, self.endDealMoney)
            #     ))
            # print("    max: {0:6.3f} {1:^28.28s}   {2:6.3f} {3:^28.28s}   {4:6.3f} {5:28s}".format(
            #     self.priceToPrediction(self.sixtyPredictionSpread['max']),
            #     self.getChangePredictionDebug(self.sixtyPredictionSpread['max'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.ninetyPredictionSpread['max']),
            #     self.getChangePredictionDebug(self.ninetyPredictionSpread['max'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.oneTwentyPredictionSpread['max']),
            #     self.getChangePredictionDebug(self.oneTwentyPredictionSpread['max'], self.endAmount, self.endDealMoney)
            #     ))
            # print("    std:     {0:10.3f}                            {1:10.3f}                            {2:10.3f}".format(
            #     self.sixtyPredictionSpread['std'],
            #     self.ninetyPredictionSpread['std'],
            #     self.oneTwentyPredictionSpread['std'],
            #     ))
            # print(" betAvg: {0:6.3f} {1:^28.28s}   {2:6.3f} {3:^28.28s}   {4:6.3f} {5:28s}".format(
            #     self.priceToPrediction(self.sixtyPredictionSpread['betAvg']),
            #     "  N-A" if ( self.priceToPrediction(self.sixtyPredictionSpread['betAvg']) == 0.0 ) else self.getChangePredictionDebug(self.sixtyPredictionSpread['betAvg'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.ninetyPredictionSpread['betAvg']),
            #     "  N-A" if ( self.priceToPrediction(self.ninetyPredictionSpread['betAvg']) == 0.0 ) else self.getChangePredictionDebug(self.ninetyPredictionSpread['betAvg'], self.endAmount, self.endDealMoney),
            #     self.priceToPrediction(self.oneTwentyPredictionSpread['betAvg']),
            #     "  N-A" if ( self.priceToPrediction(self.oneTwentyPredictionSpread['betAvg']) == 0.0 ) else self.getChangePredictionDebug(self.oneTwentyPredictionSpread['betAvg'], self.endAmount, self.endDealMoney)
            #     ))





            # for prediction in self.predictionSets[::-1]:
            #     change = self.changeFormat(findDealMoney(prediction['pValue'], self.endAmount), self.endDealMoney, self.sellCoin)
            #     if self.nextActionType() == 'buy':
            #         change = self.changeFormat(findAmount(prediction['pValue'], self.endDealMoney), self.endAmount, self.buyCoin, False)
            #     longChange = self.changeFormat(findDealMoney(prediction['pLongValue'], self.endAmount), self.endDealMoney, self.sellCoin)
            #     if self.nextActionType() == 'buy':
            #         longChange = self.changeFormat(findAmount(prediction['pLongValue'], self.endDealMoney), self.endAmount, self.buyCoin, False)
            #     print('{0:s}: short: {1:6.2f}  {2:6.2f} {3:s}    long: {4:6.2f}  {5:6.2f} {6:s} '.format(prediction['string'], prediction['value'], prediction['pValue'], change, prediction['longValue'], prediction['pLongValue'], longChange))

            # change = self.changeFormat(findDealMoney(self.avgPricePrediction, self.endAmount), self.endDealMoney, self.sellCoin)
            # if self.nextActionType() == 'buy':
            #     change = self.changeFormat(findAmount(self.avgPricePrediction, self.endDealMoney), self.endAmount, self.buyCoin, False)
            # longChange = self.changeFormat(findDealMoney(self.avgLongPricePrediction, self.endAmount), self.endDealMoney, self.sellCoin)
            # if self.nextActionType() == 'buy':
            #     longChange = self.changeFormat(findAmount(self.avgLongPricePrediction, self.endDealMoney), self.endAmount, self.buyCoin, False)
            # print("current avg short: {0:6.2f}  {1:6.2f} {2:s}    long: {3:6.2f}  {4:6.2f} {5:s}".format(self.avgPrediction, self.avgPricePrediction, change, self.avgLongPrediction, self.avgLongPricePrediction, longChange))

            # if len(self.avgPricePredictionSets):
            #     # shortPredictionSetsOrdered = self.avgPricePredictionSets
            #     # filter(isStillValid, shortPredictionSetsOrdered)
            #     # shortPredictionSetsOrdered.sort(key=lambda item: item['value'] )
            #     # minPrediction = shortPredictionSetsOrdered[0]
            #     # midPrediction = shortPredictionSetsOrdered[round(len(shortPredictionSetsOrdered)/2)]
            #     # maxPrediction = shortPredictionSetsOrdered[-1]

            #     # predictionSetsOrdered = sorted(self.avgPricePredictionSets, key=lambda item: item['longValue'] )
            #     # minLongPrediction = predictionSetsOrdered[0]
            #     # midLongPrediction = predictionSetsOrdered[round(len(predictionSetsOrdered)/2)]
            #     # maxLongPrediction = predictionSetsOrdered[-1]


            #     change = self.changeFormat(findDealMoney(self.minPrediction['value'], self.endAmount), self.endDealMoney, self.sellCoin)
            #     if self.nextActionType() == 'buy':
            #         change = self.changeFormat(findAmount(self.minPrediction['value'], self.endDealMoney), self.endAmount, self.buyCoin, False)
            #     longChange = self.changeFormat(findDealMoney(self.minLongPrediction['longValue'], self.endAmount), self.endDealMoney, self.sellCoin)
            #     if self.nextActionType() == 'buy':
            #         longChange = self.changeFormat(findAmount(self.minLongPrediction['longValue'], self.endDealMoney), self.endAmount, self.buyCoin, False)
            #     print('min short : {6:6.2f} {0:6.2f} {1:s} expires: {5:s}     long: {7:6.2f} {2:6.2f} {3:s} expires: {4:s} '.format(self.minPrediction['value'], change, self.minLongPrediction['longValue'], longChange, self.minLongPrediction['longExpires'].astimezone(timezone('US/Central')).strftime("%I:%M:%S%p"), self.minPrediction['expires'].astimezone(timezone('US/Central')).strftime("%I:%M:%S%p"), self.priceToPrediction(self.minPrediction['value']), self.priceToPrediction(self.minLongPrediction['longValue'])))

            #     change = self.changeFormat(findDealMoney(self.midPrediction['value'], self.endAmount), self.endDealMoney, self.sellCoin)
            #     if self.nextActionType() == 'buy':
            #         change = self.changeFormat(findAmount(self.midPrediction['value'], self.endDealMoney), self.endAmount, self.buyCoin, False)
            #     longChange = self.changeFormat(findDealMoney(self.midLongPrediction['longValue'], self.endAmount), self.endDealMoney, self.sellCoin)
            #     if self.nextActionType() == 'buy':
            #         longChange = self.changeFormat(findAmount(self.midLongPrediction['longValue'], self.endDealMoney), self.endAmount, self.buyCoin, False)
            #     print('mid short : {6:6.2f} {0:6.2f} {1:s} expires: {5:s}     long: {7:6.2f} {2:6.2f} {3:s} expires: {4:s} '.format(self.midPrediction['value'], change, self.midLongPrediction['longValue'], longChange, self.midLongPrediction['longExpires'].astimezone(timezone('US/Central')).strftime("%I:%M:%S%p"), self.midPrediction['expires'].astimezone(timezone('US/Central')).strftime("%I:%M:%S%p"), self.priceToPrediction(self.midPrediction['value']), self.priceToPrediction(self.midLongPrediction['longValue'])))

            #     change = self.changeFormat(findDealMoney(self.avgPredictionValue, self.endAmount), self.endDealMoney, self.sellCoin)
            #     if self.nextActionType() == 'buy':
            #         change = self.changeFormat(findAmount(self.avgPredictionValue, self.endDealMoney), self.endAmount, self.buyCoin, False)
            #     longChange = self.changeFormat(findDealMoney(self.avgLongPredictionValue, self.endAmount), self.endDealMoney, self.sellCoin)
            #     if self.nextActionType() == 'buy':
            #         longChange = self.changeFormat(findAmount(self.avgLongPredictionValue, self.endDealMoney), self.endAmount, self.buyCoin, False)
            #     print('avg short : {6:6.2f} {0:6.2f} {1:s}                         long: {7:6.2f} {2:6.2f} {3:s}  '.format(self.avgPredictionValue, change, self.avgLongPredictionValue, longChange, 'x', 'x', self.priceToPrediction(self.avgPredictionValue), self.priceToPrediction(self.avgLongPredictionValue)))

            #     change = self.changeFormat(findDealMoney(self.maxPrediction['value'], self.endAmount), self.endDealMoney, self.sellCoin)
            #     if self.nextActionType() == 'buy':
            #         change = self.changeFormat(findAmount(self.maxPrediction['value'], self.endDealMoney), self.endAmount, self.buyCoin, False)
            #     longChange = self.changeFormat(findDealMoney(self.maxLongPrediction['longValue'], self.endAmount), self.endDealMoney, self.sellCoin)
            #     if self.nextActionType() == 'buy':
            #         longChange = self.changeFormat(findAmount(self.maxLongPrediction['longValue'], self.endDealMoney), self.endAmount, self.buyCoin, False)
            #     print('max short : {6:6.2f} {0:6.2f} {1:s} expires: {5:s}     long: {7:6.2f} {2:6.2f} {3:s} expires: {4:s} '.format(self.maxPrediction['value'], change, self.maxLongPrediction['longValue'], longChange, self.maxLongPrediction['longExpires'].astimezone(timezone('US/Central')).strftime("%I:%M:%S%p"), self.maxPrediction['expires'].astimezone(timezone('US/Central')).strftime("%I:%M:%S%p"), self.priceToPrediction(self.maxPrediction['value']), self.priceToPrediction(self.maxLongPrediction['longValue'])))
            #
            #
            #
            #
            #
            #
            #
            #
            #



            # toGreen = getFloatFormat(abs(self.greenPrice - self.priceData['currentPrice']))
            # print("distanceToGreen: {0:{1}.{2}f}".format(toGreen[0], toGreen[1], toGreen[2]))


            # unscaledLean = buyTime / sellTime
            # print("buy min: {0:0.0f} sell min: {1:0.0f}  unscaledLean: {2:0.2f}".format(buyTime, sellTime, unscaledLean))
            # self.getAccountValue()
            # if self.currencyLean > 1.0:
            #     toward = ' wait for profit'
            # else:
            #     toward = ' faster trade '
            # print("currencyLean: {0:0.6f} toward {1:s}".format(self.currencyLean, toward))
            # if self.historyLean > 1.0:
            #     toward = ' wait for profit'
            # else:
            #     toward = ' faster trade '
            # print(" historyLean: {0:0.6f} toward {1:s}".format(self.historyLean, toward))
            # if self.recentRangeLean > 1.0:
            #     toward = ' wait for profit'
            # else:
            #     toward = ' faster trade '
            # print("  rRangeLean: {0:0.6f} toward {1:s}".format(self.recentRangeLean, toward))
            # if self.timeLean > 1.0:
            #     toward = ' wait for profit'
            # else:
            #     toward = ' faster trade '
            # print("    timeLean: {0:0.6f} toward {1:s}".format(self.timeLean, toward))
            # if self.greenTouchesLean > 1.0:
            #     toward = ' wait for profit'
            # else:
            #     toward = ' faster trade '
            # print("  greenTLean: {0:0.6f} toward {1:s}".format(self.greenTouchesLean, toward))
            # since = now - self.lastTime
            # s1 = since.total_seconds()
            # hours1, remainder = divmod(s1, 3600)
            # minutes1, seconds = divmod(remainder, 60)
            # print('{:02}hr {:02}m since last '.format(int(hours1), int(minutes1)) + self.lastAction)
            # since = now - self.listedTime
            # s2 = since.total_seconds()
            # hours2, remainder = divmod(s2, 3600)
            # minutes2, seconds = divmod(remainder, 60)
            # print('{:02}hr {:02}m since last '.format(int(hours2), int(minutes2)) + self.listedType + ' trade put')
            # since = now - self.actionTime
            # s3 = since.total_seconds()
            # hours3, remainder = divmod(s3, 3600)
            # minutes3, seconds = divmod(remainder, 60)
            # print('{:02}hr {:02}m since last '.format(int(hours3), int(minutes3)) + 'actionTime ')
            # since = now - self.goalTime
            # s4 = since.total_seconds()
            # hours4, remainder = divmod(s4, 3600)
            # minutes4, seconds = divmod(remainder, 60)
            # print('{:02}hr {:02}m since last '.format(int(hours4), int(minutes4)) + 'goalTime ')
            # since = now - self.resetTime
            # s5 = since.total_seconds()
            # hours5, remainder = divmod(s5, 3600)
            # minutes5, seconds = divmod(remainder, 60)
            # print('{:02}hr {:02}m since last '.format(int(hours5), int(minutes5)) + 'resetTime ')
            # if self.latestPrice > self.lastPrice:
            #     print("price is up since last trade by:")
            #     change = self.latestPrice - self.lastPrice
            #     print("      %0.8f" %change)
            # else:
            #     print("price is down since last trade by:")
            #     change = self.lastPrice - self.latestPrice
            #     print("      %0.8f" %change)

            # if self.nextActionType() == 'sell':
            #     print("sellActionPrice: %0.8f" %self.priceData['sellActionPrice'])
            # else:
            #     print(" buyActionPrice: %0.8f" %self.priceData['buyActionPrice'])
            # print(" avgActionPrice: %0.8f %d trades" %(self.priceData['avgActionPrice'], self.priceData['actionCount'])) # current price farthest toward profit
            # print("    latestPrice: %0.8f" %self.latestPrice) # current price farthest toward profit

            # print("      lastPrice: %0.8f" %self.lastPrice) # price most recently completed in a trade
            # color=''
            # if self.nextActionType() == 'sell':
            #     if self.actionPrice > self.greenPrice:
            #         color=bcolors.OKGREEN
            #     else:
            #         color=bcolors.WARNING
            # else:
            #     if self.actionPrice < self.greenPrice:
            #         color=bcolors.OKGREEN
            #     else:
            #         color=bcolors.WARNING
            # print("%s    actionPrice: %0.8f %0.1fp %s" %(color, self.actionPrice, self.percToGreen(self.actionPrice), bcolors.ENDC)) # price we will sell at post hook
            # print("     guidePrice: %0.8f %0.1fp" %(self.guidePrice, self.percToGreen(self.guidePrice))) # price farthest toward profit since last action
            # goalPercent = self.goalPercent * 100
            # print("      goalPrice: {0:0.8f} {1:0.1f}p {2:0.2f}%".format(self.goalPrice, self.percToGreen(self.goalPrice), goalPercent)) # price we will sit at pre hook
            # formatted = getFloatFormat(self.listedPrice)
            # print("    listedPrice: {0:{1}.{2}f} {3:0.1f}p".format(formatted[0], formatted[1], formatted[2], self.percToGreen(self.listedPrice))) # price most recently placed in a trade

            # goalDebug = self.goalPercentReset
            # print("goalDebug {0:0.5f} inital ".format(goalDebug))
            # goalDebug = goalDebug * self.timeLean
            # print("goalDebug {0:0.5f} timeLean {1:0.5f} DISABLED".format(goalDebug, self.timeLean))
            # goalDebug = goalDebug * self.currencyLean
            # print("goalDebug {0:0.5f} currencyLean {1:0.5f}".format(goalDebug, self.currencyLean))
            # goalDebug = goalDebug * self.historyLean
            # print("goalDebug {0:0.5f} historyLean {1:0.5f}".format(goalDebug, self.historyLean))
            # goalDebug = goalDebug * self.recentRangeLean
            # print("goalDebug {0:0.5f} recentRangeLean {1:0.5f}".format(goalDebug, self.recentRangeLean))
            # # goalDebug = goalDebug * self.greenTouchesLean
            # print("goalDebug {0:0.5f} greenTouchesLean {1:0.5f} DISABLED final".format(goalDebug, self.greenTouchesLean))

            # period = getCalcTrades(self.market, 10)
            # oneMin = getCalcTrades(self.market, 'oneMin')
            # threeMin = getCalcTrades(self.market, 'threeMin')
            # fiveMin = getCalcTrades(self.market, 'fiveMin')
            # tenMin = getCalcTrades(self.market, 'tenMin')
            # fifteenMin = getCalcTrades(self.market, 'fifteenMin')
            # thirtyMin = getCalcTrades(self.market, 'thirtyMin')

            # print('      {0:15s} {1:15s} {2:15s} {3:15s} {4:15s} {5:15s} {6:15s}'.format('period', 'oneMin', 'threeMin', 'fiveMin', 'tenMin', 'fifteenMin', 'thirtyMin'),flush=True)
            # print('  avg {0:15.4f} {1:15.4f} {2:15.4f} {3:15.4f} {4:15.4f} {5:15.4f} {6:15.4f}'.format(period['avgPrice'], oneMin['avgPrice'], threeMin['avgPrice'], fiveMin['avgPrice'], tenMin['avgPrice'], fifteenMin['avgPrice'], thirtyMin['avgPrice']),flush=True)
            # print(' high {0:15.4f} {1:15.4f} {2:15.4f} {3:15.4f} {4:15.4f} {5:15.4f} {6:15.4f}'.format(period['highPrice'], oneMin['highPrice'], threeMin['highPrice'], fiveMin['highPrice'], tenMin['highPrice'], fifteenMin['highPrice'], thirtyMin['highPrice']),flush=True)
            # print('  low {0:15.4f} {1:15.4f} {2:15.4f} {3:15.4f} {4:15.4f} {5:15.4f} {6:15.4f}'.format(period['lowPrice'], oneMin['lowPrice'], threeMin['lowPrice'], fiveMin['lowPrice'], tenMin['lowPrice'], fifteenMin['lowPrice'], thirtyMin['lowPrice']),flush=True)
            # print('start {0:15.4f} {1:15.4f} {2:15.4f} {3:15.4f} {4:15.4f} {5:15.4f} {6:15.4f}'.format(period['startPrice'], oneMin['startPrice'], threeMin['startPrice'], fiveMin['startPrice'], tenMin['startPrice'], fifteenMin['startPrice'], thirtyMin['startPrice']),flush=True)
            # print('  end {0:15.4f} {1:15.4f} {2:15.4f} {3:15.4f} {4:15.4f} {5:15.4f} {6:15.4f}'.format(period['endPrice'], oneMin['endPrice'], threeMin['endPrice'], fiveMin['endPrice'], tenMin['endPrice'], fifteenMin['endPrice'], thirtyMin['endPrice']),flush=True)
            # print('rChng {0:15.4f} {1:15.4f} {2:15.4f} {3:15.4f} {4:15.4f} {5:15.4f} {6:15.4f}'.format(period['changeReal'], oneMin['changeReal'], threeMin['changeReal'], fiveMin['changeReal'], tenMin['changeReal'], fifteenMin['changeReal'], thirtyMin['changeReal']),flush=True)
            # print('pChng {0:15.4f} {1:15.4f} {2:15.4f} {3:15.4f} {4:15.4f} {5:15.4f} {6:15.4f}'.format(period['changePercent'], oneMin['changePercent'], threeMin['changePercent'], fiveMin['changePercent'], tenMin['changePercent'], fifteenMin['changePercent'], thirtyMin['changePercent']),flush=True)
            # print('rTrvl {0:15.4f} {1:15.4f} {2:15.4f} {3:15.4f} {4:15.4f} {5:15.4f} {6:15.4f}'.format(period['travelReal'], oneMin['travelReal'], threeMin['travelReal'], fiveMin['travelReal'], tenMin['travelReal'], fifteenMin['travelReal'], thirtyMin['travelReal']),flush=True)
            # print('pTrvl {0:15.4f} {1:15.4f} {2:15.4f} {3:15.4f} {4:15.4f} {5:15.4f} {6:15.4f}'.format(period['travelPercent'], oneMin['travelPercent'], threeMin['travelPercent'], fiveMin['travelPercent'], tenMin['travelPercent'], fifteenMin['travelPercent'], thirtyMin['travelPercent']),flush=True)
            # print('  vol {0:15.4f} {1:15.4f} {2:15.4f} {3:15.4f} {4:15.4f} {5:15.4f} {6:15.4f}'.format(period['volumeAvg'], oneMin['volumeAvg'], threeMin['volumeAvg'], fiveMin['volumeAvg'], tenMin['volumeAvg'], fifteenMin['volumeAvg'], thirtyMin['volumeAvg']),flush=True)
            # print('tPmin {0:15.4f} {1:15.4f} {2:15.4f} {3:15.4f} {4:15.4f} {5:15.4f} {6:15.4f}'.format(period['tradesPrMin'], oneMin['tradesPrMin'], threeMin['tradesPrMin'], fiveMin['tradesPrMin'], tenMin['tradesPrMin'], fifteenMin['tradesPrMin'], thirtyMin['tradesPrMin']),flush=True)
            # period + 'avgPrice' : avgPrice,
            # period + 'highPrice' : highPrice,
            # period + 'lowPrice' : lowPrice,
            # period + 'startPrice' : startPrice,
            # period + 'endPrice' : endPrice,
            # period + 'changeReal' : changeReal,
            # period + 'changePercent' : changePercent,
            # period + 'travelReal' : travelReal,
            # period + 'travelPercent' : travelPercent,
            # period + 'volumeAvg' : volumeAvg
            # period + 'trades' : len(trades)
        except UnboundLocalError:
            print("No trades in 48 hrs")



        print(" ")
        print(now.astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p"))
        print(" ")
        if self.mode != 'wait':
            print(bcolors.ENDC)

        print(now.strftime("%Y-%m-%d %H:%M:%S"), flush=True)

    def percToGreen(self, price):
        return 0 if (self.greenPrice == 0) else (price * 100 / self.greenPrice)

    def getChangePredictionDebug(self, price, amount, dealMoney):
        if self.nextActionType() == 'sell':
            return str(self.changeFormat(findDealMoney(price, amount), dealMoney, self.sellCoin, True))
        return str(self.changeFormat(findAmount(price, dealMoney), amount, self.buyCoin, True))

def percChange(end, start):
    if start == 0:
        start = 0.00000001
    perc = ( end * 100 / start ) - 100
    if round(perc, 2) == 0.0:
        return bcolors.ENDC+'{0:3.2f}%'.format(0)+'      0'+bcolors.ENDC
    elif perc > 0:
        diff = getFloatFormat(end - start)
        return bcolors.OKGREEN+' {0:3.2f}% {1:{2}.{3}f}'.format(perc, diff[0], diff[1], diff[2])+bcolors.ENDC
    diff = getFloatFormat(start - end)
    return bcolors.FAIL+'{0:3.2f}% {1:{2}.{3}f}'.format(perc, diff[0], diff[1], diff[2])+bcolors.ENDC


def findDealMoney(price, amount):
    return amount * price

def findAmount(price, dealMoney):
    return 0 if ( price == 0 ) else ( dealMoney / price )

def findPrice(dealMoney, amount):
    return 0 if ( amount == 0 ) else ( dealMoney / amount )

def getFloatFormat(value, desired = 10):
    if value > 0:
        digits = int(math.log10(value))+1
    elif value == 0:
        digits = 1
    else:
        digits = int(math.log10(-value))+1

    if digits < round(desired * 0.4):
        digits = round(desired * 0.4)

    decimals = desired - digits
    return value, digits, decimals

def scale(unscaledNum, minAllowed, maxAllowed, min, max):
    return (maxAllowed - minAllowed) * (unscaledNum - min) / (max - min) + minAllowed

def boolColor(boolean, trueGreen = True):
    if boolean:
        if trueGreen:
            return bcolors.OKGREEN
        else:
            return bcolors.FAIL
    else:
        return bcolors.ENDC

class bcolors:
    HEADER = '\033[95m'
    # OKBLUE = '\033[94m'
    OKBLUE = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def get_account(trader):
    request_client = RequestClient()
    response = request_client.request('GET', '{url}/v1/balance/info'.format(url=request_client.url))
    latest = complex_json.loads(response.data)
    if trader.sellCoin in latest['data']:
        trader.available[trader.sellCoin] = float(latest['data'][trader.sellCoin]['available'])
        trader.frozen[trader.sellCoin] = float(latest['data'][trader.sellCoin]['frozen'])
        # print(sellCoin + " Balance Found: ", Bot.available[sellCoin])
    else:
        trader.available[trader.sellCoin] = 0.0
    if trader.buyCoin in latest['data']:
        trader.available[trader.buyCoin] = float(latest['data'][trader.buyCoin]['available'])
        trader.frozen[trader.buyCoin] = float(latest['data'][trader.buyCoin]['frozen'])
        # print(buyCoin + " Balance Found: ", Bot.available[buyCoin])
    else:
        trader.available[trader.buyCoin] = 0.0
    if 'CET' in latest['data']:
        trader.available['CET'] = float(latest['data']['CET']['available'])
    else:
        trader.available['CET'] = 0.0
    # print(complex_json.dumps(latest['data'], indent = 4, sort_keys=True))

def get_order_details(id):
    global callTime
    request_client = RequestClient()
    params = {
        'id': id,
        'page': 1,
        'limit': 100
    }
    response = request_client.request(
            'GET',
            '{url}/v1/order/deals'.format(url=request_client.url),
            params=params
    )
    try:
        latest = complex_json.loads(response.data)
        print(complex_json.dumps(latest['data'], indent = 4, sort_keys=True))
        return latest['data']['data'][0]
    except AttributeError:
        print("Internet connectivity error, get_order_details", flush=True)
        exit()
    return False

def get_latest(trader, market, limit=1000):
    global callTime
    request_client = RequestClient()
    params = {
        'market': market,
        'limit': limit
    }
    response = request_client.request(
            'GET',
            '{url}/v1/market/deals'.format(url=request_client.url),
            params=params
    )
    try:
        latest = complex_json.loads(response.data)
        # print(complex_json.dumps(latest, indent = 4, sort_keys=True))
    except AttributeError:
        print("Internet connectivity error, get_latest", flush=True)
        exit()

    lowPrice = 99999999999999.9
    highPrice = 0.0
    timeRange = 99999999999999
    currentTime = 0
    currentPrice = 0
    startTime = 9999999999999999
    buyActionPrice = 0.0
    sellActionPrice = 0.0
    avgActionPrice=0.0
    avgCount=0
    thisFirstTrade = 0
    thisTime=str(time.time())

    if market == trader.market:
        addTrades(latest['data'], trader)

    for array in latest['data']:
        if currentPrice == 0:
            currentPrice = float(array['price'])
            currentTime = int(array['date'])
            thisFirstTrade = currentTime

        if trader.lastFirstTrade == 0:
            trader.lastFirstTrade = int(array['date'])

        if ( int(array['date']) >= (math.floor(time.time()) - callTime[trader.mode])
                or int(array['date']) >= trader.lastFirstTrade):

            if sellActionPrice == 0.0:
                sellActionPrice = float(array['price'])
                buyActionPrice = float(array['price'])

            if float(array['price']) < buyActionPrice:
                buyActionPrice = float(array['price'])

            if float(array['price']) > sellActionPrice:
                sellActionPrice = float(array['price'])

            avgActionPrice = float(array['price']) + avgActionPrice
            avgCount = 1 + avgCount

        if int(array['date']) > currentTime:
            currentPrice = float(array['price'])
            currentTime = int(array['date'])

        if float(array['price']) > highPrice:
            highPrice = float(array['price'])

        if float(array['price']) < lowPrice:
            lowPrice = float(array['price'])

        if int(array['date']) < startTime:
            startTime = int(array['date'])
            timeRange = datetime.fromtimestamp(currentTime) - datetime.fromtimestamp(startTime)
            startPrice = float(array['price'])

    if sellActionPrice == 0.0:
        sellActionPrice = currentPrice
    if buyActionPrice == 0.0:
        buyActionPrice = currentPrice

    if avgCount > 1:
        avgActionPrice = avgActionPrice / avgCount

    if avgActionPrice == 0.0:
        avgActionPrice = currentPrice

    if market == trader.market:
        trader.lastFirstTrade = thisFirstTrade

    return {
        'sellActionPrice' : sellActionPrice,
        'buyActionPrice' : buyActionPrice,
        'avgActionPrice' : avgActionPrice,
        'actionCount': avgCount,
        'timeRange': timeRange,
        'lowPrice': lowPrice,
        'highPrice': highPrice,
        'startPrice': startPrice,
        'currentPrice': currentPrice
    }

#

def addTrades(newTrades, trader):
    values = ''
    outsideValues = ''
    for trade in newTrades:
        values += '({0:d},"{1:s}",{2:d},{3:d},"{4:s}","{5:s}","{6:s}"),'.format(trade['id'], trade['amount'], trade['date'], trade['date_ms'], trade['price'], trade['type'], trader.market)
        outsideValues += "({0:d}, {1:f}, {2:f}, {3:f}, {4:f}, {5:f}, {6:f}),".format(trade['id'], trader.coinbasePrice, trader.binancePrice, trader.huobiPrice, trader.bittrexPrice, trader.bchPrice, trader.ethPrice)
    cur.execute(
        "INSERT IGNORE INTO outsidePrices(tradeId, coinbasePrice, binancePrice, huobiPrice, bittrexPrice, BCHprice, ETHprice) VALUES "+outsideValues[:-1])
    conn.commit()
    cur.execute(
        "INSERT IGNORE INTO trades(id, amount, date, date_ms, price, type, market) VALUES " + values[:-1])
    conn.commit()
    (trades, tSecAvg, fiveAvg, thirtyAvg, oneTwentyAvg) = liveCruncher.getLatestNumbers(trader.trades)
    trader.trades = trades
    trader.avgSets.append({
        'tSecAvg' : float(tSecAvg),
        'fiveAvg' : float(fiveAvg),
        'thirtyAvg' : float(thirtyAvg),
        'oneTwentyAvg' : float(oneTwentyAvg)
    })
    trader.r.lpush('avgSets', pickle.dumps({
        'tSecAvg' : float(tSecAvg),
        'fiveAvg' : float(fiveAvg),
        'thirtyAvg' : float(thirtyAvg),
        'oneTwentyAvg' : float(oneTwentyAvg)
    }))



def order_pending(market):
    request_client = RequestClient()
    params = {
        'market': market,
        'page': 1,
        'limit': 1
    }
    response = request_client.request(
            'GET',
            '{url}/v1/order/pending'.format(url=request_client.url),
            params=params
    )

    try:
        latest = complex_json.loads(response.data)
    except AttributeError:
        print("Internet connectivity error, order_pending", flush=True)
        exit()


    result = {
        'orderUp': False,
        'amount': 0,
        'remaining': 0,
        'id': 0
    }
    try:
        if latest['data']['count'] == 0:
            return result

        if Bot.listedPrice == 0.0:
            Bot.listedPrice=float(latest['data']['data'][0]['price'])
        result['amount']=latest['data']['data'][0]['amount']
        result['remaining']=latest['data']['data'][0]['left']
        result['id']=latest['data']['data'][0]['id']
        result['orderUp']=True
    except IndexError:
        result['orderUp']=False
    except KeyError:
        result['orderUp']=False
    return result



def order_finished(market, page, limit, trader):
    request_client = RequestClient()
    params = {
        'market': market,
        'page': page,
        'limit': limit
    }
    response = request_client.request(
            'GET',
            '{url}/v1/order/finished'.format(url=request_client.url),
            params=params
    )
    latest = complex_json.loads(response.data)
    try:
        # print(complex_json.dumps(latest, indent = 4, sort_keys=True), flush=True)
        for data in latest['data']['data']:
            cur.execute("SELECT role FROM orders WHERE tradeId = {0:d}".format(data['id']))
            results = cur.fetchall()
            for result in results:
                if result[0] != 'maker' and result[0] != 'taker':
                    print("Run lookup and update")
                    details = get_order_details(data['id'])
                    # print(complex_json.dumps(details, indent = 4, sort_keys=True), flush=True)
                    cur.execute(
                            "UPDATE orders SET status = '{0:s}', finished_time = '{2:s}', deal_amount = {3:f}, deal_money = {4:f}, fee = {5:f}, maker_fee = {6:f}, taker_fee = {7:f}, role = '{8:s}', fee_asset = '{9:s}', account_value_usd = {10:f} WHERE tradeId = {1:d}".format(data['status'], data['id'], datetime.fromtimestamp(data['finished_time']).strftime("%Y-%m-%d %H:%M:%S"), float(data['deal_amount']), float(data['deal_money']), float(data['deal_fee']), float(data['maker_fee_rate']), float(data['taker_fee_rate']), details['role'], details['fee_asset'], trader.getAccountValue()))
                    conn.commit()

        if market == trader.market:
            # print('<><><><><><>latest status: '+latest['data']['data'][0]['status'])

            if latest['data']['data'][0]['status'] == 'done':
                if latest['data']['data'][0]['type'] == 'sell':
                    if trader.lastAction != 'sell':
                        cur.execute(
                            "UPDATE orders SET greenTouches = {0:d} WHERE tradeId = {1:d}".format(trader.greenTouches, int(latest['data']['data'][0]['id'])))
                        conn.commit()
                        trader.greenTouches = 0
                        trader.redTouches = 0
                        trader.resetTime = datetime.fromtimestamp(latest['data']['data'][0]['finished_time'])
                        trader.listedType = 'none'
                    trader.lastAction = 'sell'
                elif latest['data']['data'][0]['type'] == 'buy':
                    if trader.lastAction != 'buy':
                        trader.greenTouches = 0
                        trader.redTouches = 0
                        trader.resetTime = datetime.fromtimestamp(latest['data']['data'][0]['finished_time'])
                        trader.listedType = 'none'
                    trader.lastAction = 'buy'
                # print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> lastPrice set to '+latest['data']['data'][0]['avg_price'])
                trader.lastPrice = float(latest['data']['data'][0]['avg_price'])
                trader.lastTime = datetime.fromtimestamp(latest['data']['data'][0]['finished_time'])
                trader.completedTradeId = latest['data']['data'][0]['id']
                trader.tradeInProgress = False
                get_account(trader)
                trader.newHistory[latest['data']['data'][0]['id']] = {
                    'type' : latest['data']['data'][0]['type'],
                    'time' : datetime.fromtimestamp(latest['data']['data'][0]['finished_time']),
                    'price' : float(latest['data']['data'][0]['avg_price']),
                    'amount' : float(latest['data']['data'][0]['amount']),
                    'dealMoney' : float(latest['data']['data'][0]['deal_money']),
                    'tradeId' : latest['data']['data'][0]['id'],
                    'accountValue' : trader.getAccountValue()
                }
            else:
                trader.tradeInProgress = True
                # with open('log.txt', 'a') as f:
                #     f.write(" ")
                #     f.write(datetime.now().strftime('%Y-%m-%d %I:%M %S %p'))
                #     f.write(complex_json.dumps(latest, indent = 4, sort_keys=True))
    except IndexError:
        print('Last Transaction too old, update last prices')
        now = datetime.now()
        print(" ")
        print(now.strftime("%Y-%m-%d %H:%M:%S"))
    except KeyError:
        # print(complex_json.dumps(latest, indent = 4, sort_keys=True))
        print('Last Transaction too old, update last prices')
        now = datetime.now()
        print(" ")
        print(now.strftime("%Y-%m-%d %H:%M:%S"))
    # print(complex_json.dumps(latest, indent = 4, sort_keys=True))
    return latest['data']['data']



def put_limit(data, botType, extra=''):
    extra = (extra[:253] + '..') if len(extra) > 253 else extra
    cur.execute(
        "INSERT INTO orders(bot_type, status, market, amount, price, type, bot_type_extra) VALUES ('{0:s}', '{1:s}', '{2:s}', {3:f}, {4:f}, '{5:s}', '{6:s}')".format(botType, 'requested', data['market'], data['amount'], data['price'], data['type'], extra))
    conn.commit()
    cur.execute("SELECT LAST_INSERT_ID() FROM orders")
    values = cur.fetchall()
    sourceId = values[0][0]
    data['source_id'] = str(sourceId)
    request_client = RequestClient()
    response = request_client.request(
            'POST',
            '{url}/v1/order/limit'.format(url=request_client.url),
            json=data,
    )
    latest = complex_json.loads(response.data)
    print(complex_json.dumps(latest, indent = 4, sort_keys=True))
    now = datetime.now()
    print(" ")
    print(now.strftime("%Y-%m-%d %H:%M:%S"))
    try:
        cur.execute(
            "UPDATE orders SET tradeId = {0:d}, deal_money = {1:f}, fee = {2:f}, create_time = '{3:s}', status = '{4:s}' WHERE id = {5:d}".format(latest['data']['id'],  float(latest['data']['deal_money']), float(latest['data']['deal_fee']), datetime.fromtimestamp(latest['data']['create_time']).strftime("%Y-%m-%d %H:%M:%S"), latest['data']['status'], int(latest['data']['source_id'])))
        conn.commit()
        return latest['data']['id']
    except KeyError:
        print("Trade failed with requested data")
        print(complex_json.dumps(data, indent = 4, sort_keys=True), flush=True)
        return "0"


def cancel_order(id, trader):
    request_client = RequestClient()
    data = {
        "id": id,
        "market": trader.market,
    }
    # print(complex_json.dumps(data, indent = 4, sort_keys=True))

    response = request_client.request(
            'DELETE',
            '{url}/v1/order/pending'.format(url=request_client.url),
            params=data,
    )

    latest = complex_json.loads(response.data)

    # print(complex_json.dumps(latest, indent = 4, sort_keys=True), flush=True)
    cur.execute(
        "UPDATE orders SET status = '{0:s}', finished_time = '{2:s}', account_value_usd = {3:f} WHERE tradeId = {1:d}".format('canceled', int(data['id']), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), trader.getAccountValue()))
    conn.commit()
    get_account(trader)
    return response.data

# def put_market(data):
#     request_client = RequestClient()

#     response = request_client.request(
#             'POST',
#             '{url}/v1/order/market'.format(url=request_client.url),
#             json=data,
#     )
#     print(response.data)

# def calc_order(available):

#     if orderUp == False:
#         if lastAction == 'buy':
#             executedValue = float(available['XMR']) * lastBuyPrice
#             minSellPrice = ( executedValue + ((executedValue * fee ) * minimumFeeMultiple ) ) / float(available['XMR'])
#             if currentPrice > minSellPrice:
#                 print('BONUS')
#                 print('BONUS')
#                 print('Bonus price was over min, cur: ', minSellPrice, ', ', currentPrice)
#                 print('BONUS')
#                 print('BONUS')
#                 limitPrice = currentPrice
#             else:
#                 print('minSellPrice: ', minSellPrice)
#                 limitPrice = minSellPrice
#             limitAmount = available['XMR']
#             data = {
#                 "amount": limitAmount,
#                 "price": limitPrice,
#                 "type": "sell",
#                 "market": market
#             }
#             print(complex_json.dumps(data, indent = 4, sort_keys=True))
#             put_limit(data)
#         if lastAction == 'sell':
#             maxBuyPrice = lastSellPrice - ((lastSellPrice * fee ) * minimumFeeMultiple )
#             if currentPrice < maxBuyPrice:
#                 print('BONUS')
#                 print('BONUS')
#                 print('Bonus price was under max, cur: ', maxBuyPrice, currentPrice)
#                 print('BONUS')
#                 print('BONUS')
#                 limitPrice = currentPrice
#             else:
#                 print('maxBuyPrice: ', maxBuyPrice)
#                 limitPrice = maxBuyPrice
#             limitAmount = float(available[self.sellCoin]) / limitPrice
#             data = {
#                 "amount": limitAmount,
#                 "price": limitPrice,
#                 "type": "buy",
#                 "market": market
#             }
#             print(complex_json.dumps(data, indent = 4, sort_keys=True))
#             put_limit(data)
#     else:
#         print("Order Already Pending")



if __name__ == '__main__':
    trader = Bot(buyCoin, sellCoin, resetValue)
    print("<><><><><><    BOT INIT    ><><><><><>")
    # pp = pprint.PrettyPrinter(indent=4)
    while True:
        # trader.go()
        trader.priceData = get_latest(trader, trader.market, 200)
        trader.getOutsidePrice()
        print(" ")
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        time.sleep(callTime[trader.mode])
        # currentData = get_latest(self, trader)
        # # pp.pprint(currentData['currentPrice'])
        # trader.addCurrentPrice(currentData)
        # trader.printDebug()
        # time.sleep(callTime[trader.mode])


        # if order_pending() == False:
        #     get_account(available)
        #     order_finished(1, 1)
        #     calc_order()
        #     print(" ")
        # else:
        #     # sys.stdout.write('\r')
        #     # sys.stdout.flush()
        #     Traded = float(Amount) - float(Remaining)
        #     print('No transaction ', datetime.now(), ' - XMR Amount:', Amount, ' - Traded:',Traded)

    # count = 1
    # a = time.time() * 1000
    # while True:
    #     b = time.time() * 1000
    #     order_data = complex_json.loads(put_limit())['data']
    #     id = order_data['id']
    #     market = order_data['market']
    #     cancel_order(id, market)
    #     print time.time() * 1000 - b
    #     count += 1
    #     if count >= 50:
    #         break

    # print 'avg', (time.time() * 1000 - a) / 50.0

# def getRawTrades(market, seconds = 0, quantity = 0):
#     limit = ""
#     andWhere = ""
#     if quantity != 0:
#         limit = " LIMIT 0,{0d}".format(quantity)
#     if seconds != 0:
#         targetDate = datetime.now() - timedelta(seconds=seconds)
#         andWhere = " AND date > {0:0.0f}".format(targetDate.timestamp())
#     if quantity == 0 and limit == 0:
#         limit = " LIMIT 0,200"
#     cur.execute("SELECT price, amount FROM trades WHERE market = '" + market + "'" + andWhere + " ORDER BY date DESC" + limit)
#     return cur.fetchall()

# def getCalcTrades(market, period):
#     if period == 'oneMin':
#         seconds = 60
#     elif period == 'threeMin':
#         seconds = 180
#     elif period == 'fiveMin':
#         seconds = 300
#     elif period == 'tenMin':
#         seconds = 600
#     elif period == 'fifteenMin':
#         seconds = 900
#     elif period == 'thirtyMin':
#         seconds = 1800
#     else:
#         seconds = period
#         period = 'period'
#     trades = getRawTrades(market, seconds)

#     avgPrice = 0.0 #avg Price
#     highPrice = 0.0 #high price
#     lowPrice = 0.0 #low price
#     startPrice = 0.0 #start price
#     endPrice = 0.0 #end price, Current Price
#     changeReal = 0.0 #start to end change price
#     changePercent = 0.0 #start to end change price percent
#     travelReal = 0.0 #max travel in price
#     travelPercent = 0.0 #max travel in price percent
#     volume = 0.0 #total volume
#     volumeAvg = 0.0 #
#     totalPrice = 0.0 #total
#     tradeCount = len(trades)

#     try:
#         endPrice = trades[0][0]
#         startPrice = trades[-1][0]

#         for trade in trades:
#             if trade[0] > highPrice:
#                 highPrice = trade[0]

#             if trade[0] < lowPrice or lowPrice == 0.0:
#                 lowPrice = trade[0]

#             totalPrice += trade[0]
#             volume += trade[1]

#         avgPrice = totalPrice / tradeCount
#         volumeAvg = volume / tradeCount

#         changeReal = endPrice - startPrice
#         changePercent = changeReal * 100 / startPrice

#         travelReal = highPrice - lowPrice
#         travelPercent = travelReal * 100 / lowPrice

#         tradesPrMin = tradeCount / seconds * 60
#     except IndexError:
#         avgPrice = 0.0 #avg Price
#         highPrice = 0.0 #high price
#         lowPrice = 0.0 #low price
#         startPrice = 0.0 #start price
#         endPrice = 0.0 #end price, Current Price
#         changeReal = 0.0 #start to end change price
#         changePercent = 0.0 #start to end change price percent
#         travelReal = 0.0 #max travel in price
#         travelPercent = 0.0 #max travel in price percent
#         volumeAvg = 0.0 #total volume
#         totalPrice = 0.0 #total
#         tradesPrMin = 0.0 #

#     return {
#         'avgPrice' : avgPrice,
#         'highPrice' : highPrice,
#         'lowPrice' : lowPrice,
#         'startPrice' : startPrice,
#         'endPrice' : endPrice,
#         'changeReal' : changeReal,
#         'changePercent' : changePercent,
#         'travelReal' : travelReal,
#         'travelPercent' : travelPercent,
#         'volumeAvg' : volumeAvg,
#         'tradesPrMin' : tradesPrMin
#     }


