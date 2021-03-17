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
    'wait' : 2,
    'green' : 2,
    'red' : 2,
    'in-progress' : 5,
    'first': 2
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

class Bot:
    
    mode='wait'
    lastAction='reset'
    lastPrice=0.0 # price most recently completed in a trade
    lastTime=datetime.now()
    listedPrice=0.0 # price most recently placed in a trade
    listedTime=datetime.now()
    listedType='none'
    lastestPrice=0.0 # current price farthest toward profit

    previousCurrentPrice=0.0

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

    goalPercent=5
    goalPercentReset=5
    goalPercentTime=datetime.now()
    guidePercent=1.0
    actionPercent=0.5

    sellNowMovement=0.75

    USDTlean=1.0
    BTClean=1.0
    ETHLean=1.0
    BCHlean=1.0
    currencyLean=1.0 # If buyCoin is worth more, lean > 1.0
             # If sellCoin is worth more, lean < 1.0

    trades = []

    actionTaken=''
    actionValue=0.0
    completedTradeId=""

    timeLean=1.0

    historyLean=1.0
    redTimeHours=2
    resetHardTimeHours=3
    recentRangeLean=1.0
    resetTime=datetime.now()

    greenTouchesLean=1.0
    cetPile=10300.0
    cetBuyTime=datetime.now()
    cetId=0

    lowStartPercent=0.3
    highStartPercent=0.7

    feeAsPercent=0.25
    fee=0.00126
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

    history = {}
    cetSellHistory = {}
    cetBuyHistory = {}
    currentPrice = {}

    avgSets = []
    predictionString = ''
    predictionValue = 0.0
    predictionSets = []
    avgPricePrediction = 0.0
    avgPrediction = 0.0
    avgPricePredictionSets = []

    lastFirstTrade = 0

    debugPrintRotation=0

    priceData = {
        'sellActionPrice' : "0",
        'buyActionPrice' : "0",
        'avgActionPrice': 0.0,
        'timeRange': "0",
        'lowPrice': "0",
        'highPrice': "0",
        'currentPrice': "0"
    }

    BotRow = {
        'timePeriod' : 0, # time to last row
        'periodAvgPrice' : 0.0, #avg Price since last row
        'periodHighPrice' : 0.0, #high price since last row
        'periodLowPrice' : 0.0, #low price since last row
        'periodStartPrice' : 0.0, #start price since last row
        'periodEndPrice' : 0.0, #end price since last row, Current Price
        'periodChangeReal' : 0.0, #change price since last row
        'periodChangePercent' : 0.0, #change price percent since last row
        'periodTravelReal' : 0.0, #max travel in price since last row
        'periodTravelPercent' : 0.0, #max travel in price percent since last row
        '1minAvgPrice' : 0.0, #avg Price in past 1 min
        '1minHighPrice' : 0.0, #high price in past 1 min
        '1minLowPrice' : 0.0, #low price in past 1 min
        '1minStartPrice' : 0.0, #start price in past 1 min
        '1minEndPrice' : 0.0, #end price in past 1 min, Current Price
        '1minChangeReal' : 0.0, #change price in past 1 min
        '1minChangePercent' : 0.0, #change price percent in past 1 min
        '1minTravelReal' : 0.0, #max travel in price in 1 min
        '1minTravelPercent' : 0.0, #max travel in price percent in 1 min
        '3minAvgPrice' : 0.0, #avg Price in past 3 min
        '3minHighPrice' : 0.0, #high price in past 3 min
        '3minLowPrice' : 0.0, #low price in past 3 min
        '3minStartPrice' : 0.0, #start price in past 3 min
        '3minEndPrice' : 0.0, #end price in past 3 min, Current Price
        '3minChangeReal' : 0.0, #change price in past 3 min
        '3minChangePercent' : 0.0, #change price percent in past 3 min
        '3minTravelReal' : 0.0, #max travel in price in 3 min
        '3minTravelPercent' : 0.0, #max travel in price percent in 3 min
        '5minAvgPrice' : 0.0, #avg Price in past 5 min
        '5minHighPrice' : 0.0, #high price in past 5 min
        '5minLowPrice' : 0.0, #low price in past 5 min
        '5minStartPrice' : 0.0, #start price in past 5 min
        '5minEndPrice' : 0.0, #end price in past 5 min, Current Price
        '5minChangeReal' : 0.0, #change price in past 5 min
        '5minChangePercent' : 0.0, #change price percent in past 5 min
        '5minTravelReal' : 0.0, #max travel in price in 5 min
        '5minTravelPercent' : 0.0, #max travel in price percent in 5 min
        '10minAvgPrice' : 0.0, #avg Price in past 10 min
        '10minHighPrice' : 0.0, #high price in past 10 min
        '10minLowPrice' : 0.0, #low price in past 10 min
        '10minStartPrice' : 0.0, #start price in past 10 min
        '10minEndPrice' : 0.0, #end price in past 10 min, Current Price
        '10minChangeReal' : 0.0, #change price in past 10 min
        '10minChangePercent' : 0.0, #change price percent in past 10 min
        '10minTravelReal' : 0.0, #max travel in price in 10 min
        '10minTravelPercent' : 0.0, #max travel in price percent in 10 min
        '15minAvgPrice' : 0.0, #avg Price in past 15 min
        '15minHighPrice' : 0.0, #high price in past 15 min
        '15minLowPrice' : 0.0, #low price in past 15 min
        '15minStartPrice' : 0.0, #start price in past 15 min
        '15minEndPrice' : 0.0, #end price in past 15 min, Current Price
        '15minChangeReal' : 0.0, #change price in past 15 min
        '15minChangePercent' : 0.0, #change price percent in past 15 min
        '15minTravelReal' : 0.0, #max travel in price in 15 min
        '15minTravelPercent' : 0.0, #max travel in price percent in 15 min
        '30minAvgPrice' : 0.0, #avg Price in past 30 min
        '30minHighPrice' : 0.0, #high price in past 30 min
        '30minLowPrice' : 0.0, #low price in past 30 min
        '30minStartPrice' : 0.0, #start price in past 30 min
        '30minEndPrice' : 0.0, #end price in past 30 min, Current Price
        '30minChangeReal' : 0.0, #change price in past 30 min
        '30minChangePercent' : 0.0, #change price percent in past 30 min
        '30minTravelReal' : 0.0, #max travel in price in 30 min
        '30minTravelPercent' : 0.0, #max travel in price percent in 30 min
    }

    first = True
    second = True

    def __init__(self, buyCoin, sellCoin, resetLevel):
        self.buyCoin = buyCoin
        self.sellCoin = sellCoin
        self.market=self.buyCoin + self.sellCoin
        self.lastAction=resetLevel
        self.frozen[self.buyCoin] = 0.0
        self.frozen[self.sellCoin] = 0.0
        resetTime=datetime.now() - timedelta(hours=self.redTimeHours)
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

    def go(self):
        self.actionTaken=''
        self.actionValue=0.0
        self.completedTradeId=0
        self.priceData = get_latest(self, self.market, 1000)
        if self.checkOrderPending():
            self.mode = 'in-progress'
        else:
            self.calculateLean(self.priceData)
            # pp.pprint(currentData['currentPrice'])
            self.addCurrentPrice(self.priceData)
            self.predictPrice()
            self.printDebug()
        self.previousCurrentPrice = self.priceData['currentPrice']
        if self.first or self.second:
            self.mode = 'first'
            if self.first == False:
                self.second = False
            self.first = False
        try:
            self.cur.execute(
                "INSERT INTO history (time, currentPrice, greenPrice, action, actionTaken, actionValue, tradeId, avgActionPrice, buyActionPrice, sellActionPrice, market) VALUES (?,?,?,?,?,?,?,?,?,?, ?)",
                (datetime.now(), self.priceData['currentPrice'], self.greenPrice, self.nextActionType(), self.actionTaken, self.actionValue, str(self.completedTradeId), self.priceData['avgActionPrice'], self.priceData['buyActionPrice'], self.priceData['sellActionPrice'], self.market))
            self.conn.commit()
        except mariadb.Error as e:
            print(f"Error: {e}", flush=True)

    def predictPrice(self):
        while len(self.avgSets) > 5:
            oldSet = self.avgSets.pop(0)
            print(oldSet)
            newSet = self.avgSets[-1]
            print(newSet)
            self.tSecSlope = newSet['tSecAvg'] - oldSet['tSecAvg']
            self.fiveSlope = newSet['fiveAvg'] - oldSet['fiveAvg']
            self.thirtySlope = newSet['thirtyAvg'] - oldSet['thirtyAvg']
            self.oneTwentySlope = newSet['oneTwentyAvg'] - oldSet['oneTwentyAvg']
            print(self.priceData['currentPrice'])
            print(self.tSecSlope)
            print(self.fiveSlope )
            print(self.thirtySlope)
            print(self.oneTwentySlope)
            self.predictionString = categories.getRollingString(self.priceData['currentPrice'], newSet['tSecAvg'], newSet['fiveAvg'], newSet['thirtyAvg'], newSet['oneTwentyAvg'])+'-'+categories.getSlopeString(self.tSecSlope, self.fiveSlope, self.thirtySlope, self.oneTwentySlope)
            cur.execute('SELECT value FROM predictions WHERE predictions.key = "{0:s}"'.format(self.predictionString))
            values = cur.fetchall()
            self.predictionValue = 0
            for value in values:
                self.predictionValue = value[0]
            now = datetime.now()
            then = now + timedelta(minutes=10)
            self.predictionSets.append({
                'string' : self.predictionString,
                'value' : self.predictionValue,
                'time' : now.astimezone(timezone('US/Central')).strftime("%I:%M:%S%p"),
                'pValue' : self.predictionToPrice(self.predictionValue),
                'pTime' : then.astimezone(timezone('US/Central')).strftime("%I:%M:%S%p")
            })
            while len(self.predictionSets) > 12:
                self.predictionSets.pop(0)
            total = 0.0
            for vals in self.predictionSets:
                total += vals['value']
            avg = 0 if ( len(self.predictionSets) == 0 ) else ( total / len(self.predictionSets) )
            self.avgPrediction = 0 if (len(self.predictionSets) > 5 ) else avg
            self.avgPricePrediction = self.predictionToPrice(avg)
            self.avgPricePredictionSets.append({
                'value' : self.avgPricePrediction,
                'expires' : then
            })
            filter(isStillValid, self.avgPricePredictionSets)


    def predictionToPrice(self, prediction):
        return self.priceData['currentPrice'] * ( prediction + 100 ) / 100

    def topCet(self):
        if self.cetBuyTime < datetime.now() - timedelta(minutes=10):
            get_account(self.buyCoin, self.sellCoin)
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
                self.cetId = put_limit(data)
                self.cetBuyTime = datetime.now()
                get_account(self.buyCoin, self.sellCoin)



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
        goal = goal * self.currencyLean
        goal = goal * self.historyLean
        goal = goal * self.recentRangeLean
        # goal = goal * self.greenTouchesLean
        if goal < 1.00001:
            goal = 1.00001
        return goal



    def checkOrderPending(self):
        result = order_pending(self.market)

        if self.lastAction == 'reset' or self.lastAction == 'hardReset':
            if result['id'] != "0":
                cancel_order(result['id'], self)
                result['orderUp'] = False
                self.topCet()
            if self.lastAction == 'reset':
                self.history = order_finished(self.market, 1, 100, self)
            if self.lastAction == 'reset' or self.lastAction == 'hardReset':
                get_account(self.buyCoin, self.sellCoin)
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
                self.goalPercent = self.getGoalPercent()


        if result['orderUp'] == False:
            if self.lastPrice == 0.0:
                print("No previous price to set new price", flush=True)
                self.pickNewLastPrice()
            self.mode = 'wait'
            self.tradeId = "0"
            # get price
            self.greenPrice = self.feeCalc(self.lastPrice, self.fee, self.startPeakFeeMultiple, self.nextActionType())
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
            return False

        print(bcolors.OKBLUE)
        print(" ")
        print("Trade in progress amount: "+result['amount']+" remaining: "+result['remaining'])
        print(now.astimezone(timezone('US/Central')).strftime("%Y-%m-%d %I:%M:%S%p"))
        print(bcolors.ENDC)
        print(" ")
        print(now.strftime("%Y-%m-%d %H:%M:%S"), flush=True)

        return True

    def pickNewLastPrice(self):
        if self.lastAction == 'buy':
            priceRange = float(self.priceData['highPrice']) - float(self.priceData['lowPrice'])
            newLastPrice = float(self.priceData['lowPrice']) + ( ( priceRange * ( self.lowStartPercent * 100 ) ) / 100 )
            if newLastPrice < self.lastPrice:
                self.lastPrice = newLastPrice
        else:
            priceRange = float(self.priceData['highPrice']) - float(self.priceData['lowPrice'])
            newLastPrice = float(self.priceData['lowPrice']) + ( ( priceRange * ( self.highStartPercent * 100 ) ) / 100)
            if newLastPrice > self.lastPrice:
                self.lastPrice = newLastPrice
        # print('<><><><><><>pick new last price: '+str(self.lastPrice))


    def addCurrentPrice(self, priceData):
        self.priceData=priceData
        modeWas = self.mode
        if (self.lastAction == 'buy' and
                self.priceData['currentPrice'] > self.greenPrice):
            self.mode = 'green'
        elif (self.lastAction == 'buy' and
                self.priceData['currentPrice'] < self.greenPrice):
            if self.mode != 'red':
                self.mode = 'wait'
        elif (self.lastAction == 'sell' and
                self.priceData['currentPrice'] > self.greenPrice):
            if self.mode != 'red':
                self.mode = 'wait'
        elif (self.lastAction == 'sell' and
                self.priceData['currentPrice'] < self.greenPrice):
            self.mode = 'green'

        cTo = self.priceData['highPrice'] - self.priceData['currentPrice']
        if self.nextActionType() == 'sell':
            cTo = self.priceData['currentPrice'] - self.priceData['lowPrice']
        lowHighDiff = self.priceData['highPrice'] - self.priceData['lowPrice']

        currentRangePerc = ( cTo * 100 / lowHighDiff )
        # print('currentRangePerc {0:0.8f}'.format(currentRangePerc))

        if ( datetime.now() - self.lastTime > timedelta(hours=self.redTimeHours) and
                self.mode != 'green'):
            self.mode = 'red'
            self.redTouches += 1
            lossGainPerc = ( self.lastPrice * 100 / self.priceData['currentPrice'] ) - 100
            if self.nextActionType() == 'sell':
                lossGainPerc = ( self.priceData['currentPrice'] * 100 / self.lastPrice ) - 100

            cTo = self.priceData['highPrice'] - self.priceData['currentPrice']
            if self.nextActionType() == 'sell':
                cTo = self.priceData['currentPrice'] - self.priceData['lowPrice']
            lowHighDiff = self.priceData['highPrice'] - self.priceData['lowPrice']

            currentRangePerc = ( cTo * 100 / lowHighDiff )
            # print('lossGainPerc {0:5.2f} currentRangePerc {1:5.2f}'.format(lossGainPerc, currentRangePerc), flush=True)
            if lossGainPerc < -1.0 and ( lossGainPerc > -2.0 or self.lastAction == 'sell' ) and currentRangePerc > 95.0:
                print("RED TRADE")
                self.setRedTrade()

            # Rewrite red mode to look for a new good entry point and make a red sale
            #  return to the "never move green price" maxium
            # 
            # Also set to never take a > 1.2% loss
            # 
            # Also make the trigger a % from green after time X
            # 
            # if self.lastAction == 'buy':
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

        if (datetime.now() - self.actionTime > timedelta(hours=1) and
                datetime.now() - self.goalTime > timedelta(hours=1)):
            # reboot guide price replacing action trade witha goal trade
            self.setInitalGuidePrice()
            self.updateGuidePrice(self.guidePrice)

        if ( datetime.now() - self.listedTime > timedelta(minutes=5)
                and self.listedType == 'action' ):
            self.updateGuidePrice(self.latestPrice)

        self.goalPercent = self.getGoalPercent()

        # if self.lastAction == 'sell' and self.priceData['currentPrice'] < self.greenPrice:
        #     # print("CHECKING IN GREEN BUY PREDICTION", flush=True)
        #     if self.avgPrediction > self.feeAsPercent:
        #         # print("IN GREEN TRIGGER BUY", flush=True)
        #         self.actionPrice = self.priceData['currentPrice'] - (self.feePriceUnit(self.priceData['currentPrice'], self.fee) * .001)
        #         self.setActionTrade()
        #         return
        # elif self.lastAction == 'sell' and self.avgPrediction > 3.75:
        #         print("PRICE IS LEAVING, GO FOR CATCH IT", flush=True)
        #         self.setRedTrade()
        # elif self.lastAction == 'buy' and self.priceData['currentPrice'] > self.greenPrice:
        #     # print("CHECKING IN GREEN SELL PREDICTION", flush=True)
        #     if self.avgPrediction < 0 - self.feeAsPercent:
        #         # print("IN GREEN TRIGGER SELL", flush=True)
        #         self.actionPrice = self.priceData['currentPrice'] + (self.feePriceUnit(self.priceData['currentPrice'], self.fee) * .001)
        #         self.setActionTrade()
        #         return
        # elif self.lastAction == 'buy' and self.avgPrediction < -3.75:
        #         print("PRICE IS LEAVING, GO FOR CATCH IT", flush=True)
        #         self.setRedTrade()
        # el
        if self.lastAction == 'buy':
            self.latestPrice = self.priceData['sellActionPrice']
            if self.latestPrice > self.guidePrice:
                self.updateGuidePrice(self.latestPrice)
            elif self.mode == 'green' and self.priceData['avgActionPrice'] < self.actionPrice and self.actionPrice > self.greenPrice:
                self.actionPrice = self.priceData['currentPrice'] + (self.feePriceUnit(self.priceData['currentPrice'], self.fee) * 0.1)
                self.setActionTrade()
        elif self.lastAction == 'sell':
            self.latestPrice = self.priceData['buyActionPrice']
            if self.latestPrice < self.guidePrice:
                self.updateGuidePrice(self.latestPrice)
            elif self.mode == 'green' and self.priceData['avgActionPrice'] > self.actionPrice and self.actionPrice < self.greenPrice:
                self.actionPrice = self.priceData['currentPrice'] - (self.feePriceUnit(self.priceData['currentPrice'], self.fee) * 0.1)
                self.setActionTrade()
        else:
            if self.mode != 'green':
                self.updateGuidePrice(self.latestPrice)


    def setInitalGuidePrice(self):
        self.guidePrice = self.greenPrice
        if self.latestPrice == 0.0:
            if self.lastAction == 'sell':
                self.latestPrice = self.priceData['buyActionPrice']
            else:
                self.latestPrice = self.priceData['sellActionPrice']
        if self.lastAction == 'sell':
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
        if self.lastAction == 'buy':
            # if self.guidePrice < self.lastPrice:
            #     print("Peak price lower than last price when trying to sell")
            #     print("peak : ", self.guidePrice, "last : ", self.lastPrice, flush=True)
            #     exit()
            change=self.guidePrice - self.lastPrice
            actionPrice=self.lastPrice + ((change * ( self.actionPercent * 100 ) ) / 100 )
            if actionPrice > self.actionPrice:
                self.actionPrice = actionPrice
            self.goalPrice=self.lastPrice + ((change * ( self.goalPercent * 100 ) ) / 100 )
            self.replaceGoalTrade()
        elif self.lastAction == 'sell':
            # if self.guidePrice > self.lastPrice:
            #     print("Peak price higher than last price when trying to buy")
            #     print("peak : ", self.guidePrice, "last : ", self.lastPrice, flush=True)
            #     exit()
            change=self.lastPrice - self.guidePrice
            actionPrice=self.lastPrice - ((change * ( self.actionPercent * 100 ) ) / 100 )
            if actionPrice < self.actionPrice:
                self.actionPrice = actionPrice
            self.goalPrice=self.lastPrice - ((change * ( self.goalPercent * 100 ) ) / 100 )
            self.replaceGoalTrade()

    def replaceGoalTrade(self):
        if self.tradeId != "0":
            print(" Cancel existing order for new goal ")
            cancel_order(self.tradeId, self)
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
        newId=put_limit(data)
        self.tradeId=newId
        get_account(self.buyCoin, self.sellCoin)

    def setRedTrade(self):
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
            print(" Cancel existing order for RED ")
            cancel_order(self.tradeId, self)
        self.listedPrice = self.priceData['currentPrice']
        data = {
            "amount": self.nextAmount(self.listedPrice),
            "price": self.listedPrice,
            "type": self.nextActionType(),
            "market": self.market
        }
        self.listedTime = datetime.now()
        self.actionTime = datetime.now()
        self.listedType = 'red'
        self.actionTaken='red'
        self.actionValue=self.listedPrice
        print("Action trade")
        newId=put_limit(data)
        self.tradeId=newId
        get_account(self.buyCoin, self.sellCoin)

    def setActionTrade(self):
        if self.tradeId != "0":
            print(" Cancel existing order for ACTION ")
            cancel_order(self.tradeId, self)
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
        newId=put_limit(data)
        self.tradeId=newId
        get_account(self.buyCoin, self.sellCoin)

    def finalPriceSet(self, price):
        if self.lastAction == 'sell':
            if self.greenPrice < price:
                return self.greenPrice
            return price
        if self.greenPrice > price:
            return self.greenPrice
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
        if self.lastAction == 'sell':
            return 'buy'
        return 'sell'

    def nextAmount(self, price):
        get_account(self.buyCoin, self.sellCoin)
        if self.lastAction == 'sell':
            return self.available[self.sellCoin] / price
        return self.available[self.buyCoin]

    def toUSD(self, amount, coin):
        if coin == 'USDT':
            price = 1.0
        elif coin in self.currentPrice:
            price = self.currentPrice[coin]
        else:
            data = get_latest(self, coin + 'USDT', 1)
            price = data['currentPrice']
            self.currentPrice[coin] = price
        return '${0:5.2f}'.format(amount * price)


    def changeFormat(self, this, last, coin, toUsd = True):
        if last == 0.0:
            last = 0.0000001
        val = (this * 100 / last ) - 100
        currency = self.toUSD(abs(this - last),coin)
        if toUsd == False:
            change = abs(this - last)
            cf = getFloatFormat(change)
            currency = currency + " {0:{1}.{2}f} {3:4s}".format(cf[0], cf[1], cf[2], coin)
        if round(val, 2) == 0.0:
            return bcolors.ENDC+'{0:6.2f}%'.format(0)+'      0'+bcolors.ENDC
        elif val > 0:
            return bcolors.OKGREEN+'{0:6.2f}%'.format(val)+' '+currency+bcolors.ENDC
        return bcolors.FAIL+'{0:6.2f}%'.format(val)+' '+currency+bcolors.ENDC


    def printDebug(self):
        print(" ")
        print(" ")

        self.cetSellHistory = order_finished('CET'+self.sellCoin, 1, 100, self)
        self.cetBuyHistory = order_finished('CET'+self.buyCoin, 1, 100, self)

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
        endDealMoney = 0.0
        endAmount = 0.0
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

                    endDealMoney = float(trade['deal_money'])
                    endAmount = float(trade['amount'])
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
                        print(datetime.fromtimestamp(trade['finished_time']).astimezone(timezone('US/Central')).strftime("%I:%M%p")+' {0:5s} {1:4s} {2:{3}.{4}f} {5:3s}: {6:{7}.{8}f} {9:4s}: {10:7s} {11:15s} {12:15s}'.format(hmstr, trade['type'], formatted1[0], formatted1[1], formatted1[2], self.buyCoin, formatted2[0], formatted2[1], formatted2[2], self.sellCoin, self.toUSD(float(trade['deal_money']), self.sellCoin), dealMoney, change))
                    previous = float(trade['amount'])
                    previousPrice = float(trade['price'])
                # print('%2.0fhr %2.0fm %4s %0.8f ' %(float(hours1), float(minutes1), trade['type'], float(trade['avg_price'])))
        if self.debugPrintRotation > 5:
            for cetSellTrade in self.cetSellHistory[::-1]:
                if cetSellTrade['status'] == 'done':
                    if datetime.fromtimestamp(cetSellTrade['finished_time']) > now:
                        print(datetime.fromtimestamp(cetSellTrade['finished_time']).astimezone(timezone('US/Central')).strftime("%I:%M%p")+' %4s CET %0.0f with %4s @ %s ' %(cetSellTrade['type'], float(cetSellTrade['amount']), self.sellCoin, self.toUSD(float(cetSellTrade['deal_money']), self.sellCoin)))
            for cetBuyTrade in self.cetBuyHistory[::-1]:
                if cetBuyTrade['status'] == 'done':
                    if datetime.fromtimestamp(cetBuyTrade['finished_time']) > now:
                        print(datetime.fromtimestamp(cetBuyTrade['finished_time']).astimezone(timezone('US/Central')).strftime("%I:%M%p")+' %4s CET %0.0f with %4s @ %s ' %(cetBuyTrade['type'], float(cetBuyTrade['amount']), self.buyCoin, self.toUSD(float(cetBuyTrade['deal_money']), self.buyCoin)))
        totalChangeDeal = self.changeFormat(endDealMoney, startDealMoney, self.sellCoin)
        totalChangeAmount = self.changeFormat(endAmount, startAmount, self.buyCoin, False)
        totalChangeDeal24 = self.changeFormat(endDealMoney, startDealMoney24, self.sellCoin)
        totalChangeAmount24 = self.changeFormat(endAmount, startAmount24, self.buyCoin, False)
        totalChangeDeal12 = self.changeFormat(endDealMoney, startDealMoney12, self.sellCoin)
        totalChangeAmount12 = self.changeFormat(endAmount, startAmount12, self.buyCoin, False)
        # sdm = getFloatFormat(startDealMoney)
        # edm = getFloatFormat(endDealMoney)
        # sa = getFloatFormat(startAmount)
        # ea = getFloatFormat(endAmount)
        # print('dm start: {0:{1}.{2}f} dm end: {3:{4}.{5}f} a start: {6:{7}.{8}f} a end: {9:{10}.{11}f}'.format(sdm[0], sdm[1], sdm[2], edm[0], edm[1], edm[2], sa[0], sa[1], sa[2], ea[0], ea[1], ea[2]))
        if self.debugPrintRotation > 5:
            self.debugPrintRotation = 0
            print('              '+percChange(self.priceData['currentPrice'], previousPrice), flush=True)
        print('in 48hr '+str(total)+' trades '+str(buys)+' buys '+str(sells)+' sells')
        print(' deal money: '+totalChangeDeal +' amount: '+totalChangeAmount)
        print('in 24hr '+str(total24)+' trades '+str(buys24)+' buys '+str(sells24)+' sells')
        print(' deal money: '+totalChangeDeal24 +' amount: '+totalChangeAmount24)
        print('in 12hr '+str(total12)+' trades '+str(buys12)+' buys '+str(sells12)+' sells')
        print(' deal money: '+totalChangeDeal12 +' amount: '+totalChangeAmount12)
        monthDeal = self.changeFormat(endDealMoney, 1775.0, self.sellCoin)
        print('Rockport Goal: '+monthDeal, flush=True)
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
        print("%2.0fhr %2.0fm %4s" %(float(hours1), float(minutes1), self.nextActionType()))

        now = datetime.now()
        minutes = self.priceData['timeRange'].total_seconds() / 60
        travel = ( self.priceData['highPrice'] - self.priceData['lowPrice'] ) / minutes
        print(bcolors.ENDC+" time: {0:s} price gap pr min: {1:0.2f}".format(str(self.priceData['timeRange']),travel))
        lowFormatted = getFloatFormat(self.priceData['lowPrice'])
        highFormatted = getFloatFormat(self.priceData['highPrice'])
        percentDiff = ( self.priceData['highPrice'] * 100 / self.priceData['lowPrice'] ) - 100
        print("  low: {0:{1}.{2}f}   high: {3:{4}.{5}f}  diff: {6:0.2f}%".format(lowFormatted[0], lowFormatted[1], lowFormatted[2], highFormatted[0], highFormatted[1], highFormatted[2], percentDiff))
        cToLow = getFloatFormat(self.priceData['currentPrice'] - self.priceData['lowPrice'])
        cToHigh = getFloatFormat(self.priceData['highPrice'] - self.priceData['currentPrice'])
        lowHighDiff = getFloatFormat(self.priceData['highPrice'] - self.priceData['lowPrice'])
        print("c2low: {0:{1}.{2}f} c2high: {3:{4}.{5}f}  diff: {6:{7}.{8}f}".format(cToLow[0], cToHigh[1], cToLow[2], cToHigh[0], cToHigh[1], cToHigh[2], lowHighDiff[0], lowHighDiff[1], lowHighDiff[2]))
        startFormatted = getFloatFormat(self.priceData['startPrice'])
        endFormatted = getFloatFormat(self.priceData['currentPrice'])
        if self.priceData['currentPrice'] < self.priceData['startPrice']:
            change = bcolors.FAIL+' down: {0:0.2f}'.format(self.priceData['startPrice'] - self.priceData['currentPrice'])
        else:
            change = bcolors.OKGREEN+'   up: {0:0.2f}'.format(self.priceData['currentPrice'] - self.priceData['startPrice'])
        print("start: {0:{1}.{2}f}    end: {3:{4}.{5}f} {6:s}{7:s}".format(startFormatted[0], startFormatted[1], startFormatted[2], endFormatted[0], endFormatted[1], endFormatted[2], change, bcolors.ENDC))
        print(" current is {0:0.2f}% of recent range & {1:0.2f}% of green".format(cToLow[0] * 100 / lowHighDiff[0], self.percToGreen(self.priceData['currentPrice'])))
        try:
            print("current price fee ${0:0.2f}".format(self.feePriceUnit(self.priceData['currentPrice'], self.fee) * endAmount))
            # print("amount "+self.amount+" remaining "+self.remaining)
            
            # print("calc deal money ${0:0.2f}".format(findDealMoney(self.priceData['currentPrice'])))
            # print("previous deal money ${0:0.2f}".format(previousDealMoney))
            
            change = self.changeFormat(findDealMoney(self.priceData['currentPrice'], endAmount), endDealMoney, self.sellCoin)
            if self.nextActionType() == 'buy':
                change = self.changeFormat(findAmount(self.priceData['currentPrice'], endDealMoney), endAmount, self.buyCoin, False)
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
            change = self.changeFormat(findDealMoney(self.greenPrice, endAmount), endDealMoney, self.sellCoin)
            if self.nextActionType() == 'buy':
                change = self.changeFormat(findAmount(self.greenPrice, endDealMoney), endAmount, self.buyCoin, False)

            print(bcolors.OKGREEN+"G R E E N Price: {0:0.8f} {1:s}".format(self.greenPrice, change)) # price most recently placed in a trade
            print('{0:>15s}: {1:f}'.format("listed "+self.listedType, self.listedPrice), flush=True)
            
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
            change = self.changeFormat(findDealMoney(self.actionPrice, endAmount), endDealMoney, self.sellCoin)
            if self.nextActionType() == 'buy':
                change = self.changeFormat(findAmount(self.actionPrice, endDealMoney), endAmount, self.buyCoin, False)
            print("{0:s}    actionPrice: {1:0.8f} {2:s}{3:s}".format(color, self.actionPrice, change, bcolors.ENDC)) # price we will sell at post hook
            change = self.changeFormat(findDealMoney(self.guidePrice, endAmount), endDealMoney, self.sellCoin)
            if self.nextActionType() == 'buy':
                change = self.changeFormat(findAmount(self.guidePrice, endDealMoney), endAmount, self.buyCoin, False)
            print("     guidePrice: {0:0.8f} {1:s}".format(self.guidePrice, change))
            change = self.changeFormat(findDealMoney(self.goalPrice, endAmount), endDealMoney, self.sellCoin)
            if self.nextActionType() == 'buy':
                change = self.changeFormat(findAmount(self.goalPrice, endDealMoney), endAmount, self.buyCoin, False)
            goalPercent = self.goalPercent * 100
            print("      goalPrice: {0:0.8f} {1:s} {2:0.2f}%".format(self.goalPrice, change, goalPercent))

            print("  green touches: "+str(self.greenTouches))
            print("    red touches: "+str(self.redTouches))
            

            # print("predictions:")
            # for prediction in self.predictionSets[::-1]:
            #     change = self.changeFormat(findDealMoney(prediction['pValue'], endAmount), endDealMoney, self.sellCoin)
            #     if self.nextActionType() == 'buy':
            #         change = self.changeFormat(findAmount(prediction['pValue'], endDealMoney), endAmount, self.buyCoin, False)
            #     # change = self.changeFormat(prediction['pValue'], self.priceData['currentPrice'], self.sellCoin)
            #     print('{0:s}: {1:5.2f} {2:s}     {3:5.2f} {4:s} {5:s}'.format(prediction['string'], prediction['value'], prediction['time'], prediction['pValue'], change, prediction['pTime']))
            
            # change = self.changeFormat(findDealMoney(self.avgPricePrediction, endAmount), endDealMoney, self.sellCoin)
            # if self.nextActionType() == 'buy':
            #     change = self.changeFormat(findAmount(self.avgPricePrediction, endDealMoney), endAmount, self.buyCoin, False)
            # # change = self.changeFormat(self.avgPricePrediction, self.priceData['currentPrice'], self.sellCoin)
            # print("1 min avg prediction: {0:6.2f}     {1:5.2f} {2:s}".format(self.avgPrediction, self.avgPricePrediction, change))

            # for prediction in self.avgPricePredictionSets[::-1]:
            #     change = self.changeFormat(findDealMoney(prediction['value'], endAmount), endDealMoney, self.sellCoin)
            #     if self.nextActionType() == 'buy':
            #         change = self.changeFormat(findAmount(prediction['value'], endDealMoney), endAmount, self.buyCoin, False)
            #     # change = self.changeFormat(prediction['pValue'], self.priceData['currentPrice'], self.sellCoin)
            #     print('     avg : {0:5.2f} {1:s} {2:s}'.format(prediction['value'], change, prediction['expires'].astimezone(timezone('US/Central')).strftime("%I:%M:%S%p")))
            


            # toGreen = getFloatFormat(abs(self.greenPrice - self.priceData['currentPrice']))
            # print("distanceToGreen: {0:{1}.{2}f}".format(toGreen[0], toGreen[1], toGreen[2]))

            # if self.previousCurrentPrice > self.priceData['currentPrice']:
            #     diff = self.previousCurrentPrice - self.priceData['currentPrice']
            #     print("     price down: %0.8f" %diff)
            #     perc = diff * 100 / self.previousCurrentPrice
            #     print("     price down: %0.2f percent" %perc)
            # elif self.previousCurrentPrice < self.priceData['currentPrice']:
            #     diff = self.priceData['currentPrice'] - self.previousCurrentPrice
            #     print("       price up: %0.8f" %diff)
            #     perc = diff * 100 / self.priceData['currentPrice']
            #     print("       price up: %0.2f percent" %perc)
            # else:
            #     print("price didn't move")
            # unscaledLean = buyTime / sellTime
            # print("buy min: {0:0.0f} sell min: {1:0.0f}  unscaledLean: {2:0.2f}".format(buyTime, sellTime, unscaledLean))
            # if self.USDTlean != 1.0:
            #     if self.USDTlean > 1.0:
            #         toward = 'up / ' + self.buyCoin
            #     else:
            #         toward = 'down / ' + self.sellCoin
            #     print("      USDT lean: %0.8f toward %s" %(self.USDTlean, toward))
            # if self.BTClean != 1.0:
            #     if self.BTClean > 1.0:
            #         toward = 'up / ' + self.buyCoin
            #     else:
            #         toward = 'down / ' + self.sellCoin
            #     print("       BTC lean: %0.8f toward %s" %(self.BTClean, toward))
            # if self.BCHlean != 1.0:
            #     if self.BCHlean > 1.0:
            #         toward = 'up / ' + self.buyCoin
            #     else:
            #         toward = 'down / ' + self.sellCoin
            #     print("       BCH lean: %0.8f toward %s" %(self.BCHlean, toward))
            # if self.ETHlean != 1.0:
            #     if self.ETHlean > 1.0:
            #         toward = 'up / ' + self.buyCoin
            #     else:
            #         toward = 'down / ' + self.sellCoin
            #     print("       ETH lean: %0.8f toward %s" %(self.ETHlean, toward))
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

def getFloatFormat(value, desired = 8):
    if value > 0:
        digits = int(math.log10(value))+1
    elif value == 0:
        digits = 1
    else:
        digits = int(math.log10(-value))+1

    decimals = desired - digits

    return value, digits, decimals

def scale(unscaledNum, minAllowed, maxAllowed, min, max):
    return (maxAllowed - minAllowed) * (unscaledNum - min) / (max - min) + minAllowed

def isStillValid(predictionSet):
    if predictionSet['expires'] > datetime.now() - timedelta(minutes=10):
        return True
    return False

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

def get_account(buyCoin, sellCoin):
    request_client = RequestClient()
    response = request_client.request('GET', '{url}/v1/balance/info'.format(url=request_client.url))
    latest = complex_json.loads(response.data)
    if sellCoin in latest['data']:
        Bot.available[sellCoin] = float(latest['data'][sellCoin]['available'])
        Bot.frozen[sellCoin] = float(latest['data'][sellCoin]['frozen'])
        # print(sellCoin + " Balance Found: ", Bot.available[sellCoin])
    else:
        Bot.available[sellCoin] = 0.0
    if buyCoin in latest['data']:
        Bot.available[buyCoin] = float(latest['data'][buyCoin]['available'])
        Bot.frozen[buyCoin] = float(latest['data'][buyCoin]['frozen'])
        # print(buyCoin + " Balance Found: ", Bot.available[buyCoin])
    else:
        Bot.available[buyCoin] = 0.0
    if 'CET' in latest['data']:
        Bot.available['CET'] = float(latest['data']['CET']['available'])
    else:
        Bot.available['CET'] = 0.0
    # print(complex_json.dumps(latest['data'], indent = 4, sort_keys=True))



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
    for trade in newTrades:
        values += '({0:d},"{1:s}",{2:d},{3:d},"{4:s}","{5:s}","{6:s}"),'.format(trade['id'], trade['amount'], trade['date'], trade['date_ms'], trade['price'], trade['type'], trader.market)
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
        if market == trader.market:
            # print('<><><><><><>latest status: '+latest['data']['data'][0]['status'])
            if latest['data']['data'][0]['status'] == 'done':
                if latest['data']['data'][0]['type'] == 'sell':
                    if trader.lastAction != 'sell':
                        trader.greenTouches = 0
                        trader.redTouches = 0
                        trader.resetTime = datetime.fromtimestamp(latest['data']['data'][0]['finished_time'])
                    trader.lastAction = 'sell'
                elif latest['data']['data'][0]['type'] == 'buy':
                    if trader.lastAction != 'buy':
                        trader.greenTouches = 0
                        trader.redTouches = 0
                        trader.resetTime = datetime.fromtimestamp(latest['data']['data'][0]['finished_time'])
                    trader.lastAction = 'buy'
                # print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> lastPrice set to '+latest['data']['data'][0]['avg_price'])
                trader.lastPrice = float(latest['data']['data'][0]['avg_price'])
                trader.lastTime = datetime.fromtimestamp(latest['data']['data'][0]['finished_time'])
                trader.completedTradeId = latest['data']['data'][0]['id']
                trader.tradeInProgress = False
            else:
                trader.tradeInProgress = True
                # with open('log.txt', 'a') as f:
                #     f.write(" ")
                #     f.write(datetime.now().strftime('%Y-%m-%d %I:%M %S %p'))
                #     f.write(complex_json.dumps(latest, indent = 4, sort_keys=True))
    except IndexError:
        # print(complex_json.dumps(latest, indent = 4, sort_keys=True))
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
    return latest['data']['data']
    # print(complex_json.dumps(latest, indent = 4, sort_keys=True))


def put_limit(data):
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
        return latest['data']['id']
    except KeyError:
        print("Trade failed with requested data")
        print(complex_json.dumps(data, indent = 4, sort_keys=True), flush=True)
        exit()
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
    # print(complex_json.dumps(latest, indent = 4, sort_keys=True))
    get_account(trader.buyCoin, trader.sellCoin)
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
        trader.go()
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


