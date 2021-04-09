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


import order
import liveCruncher
import categories

import credentials

class accountProvider:

    orderUp = None

    tradeInProgress=False

    actionTaken=''
    actionValue=0.0
    completedTradeId=""

    cetPile=10300.0
    cetBuyTime=datetime.now()
    cetId=0

    lowStartPercent=0.3
    highStartPercent=0.7

    feeAsPercent=0.16
    fee=0.00126

    tradeId="0"
    remaining="0"
    amount="0"

    market=''
    buyCoin=''
    sellCoin=''

    available = {}
    frozen = {}

    newHistory = {}
    history = {}
    cetSellHistory = {}
    cetBuyHistory = {}
    currentPrice = {}

    lastFirstTrade = 0

    cacheOrderPendingSeconds = 4
    cacheDonePendingSeconds = 4
    orderPendingCacheTime = datetime.now()
    orderDoneCacheTime = datetime.now()
    orderPending = {
        'orderUp': False,
        'type': 'sell',
        'amount': 0,
        'remaining': 0,
        'price': 0,
        'id': 0
    }

    orderDone = {
        'type': 'sell',
        'amount': 0,
        'remaining': 0,
        'price': 0,
        'id': 0,
        'role': '',

    }

    r = redis.StrictRedis(host='localhost',port=6377,db=0)

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

    def isOrderUp(self, ignoreCache = False):
        if ( ignoreCache or 
            self.orderPendingCacheTime + timedelta(seconds=self.cacheOrderPendingSeconds) < datetime.now() ):
            self.orderPendingCacheTime = datetime.now()
            self.orderPending = self.order_pending(self.market)
        return self.orderPending['orderUp']

    def isOrderInProgress(self, ignoreCache = False):
        if ( ignoreCache or 
            self.orderPendingCacheTime + timedelta(seconds=self.cacheOrderPendingSeconds) < datetime.now() ):
            self.orderPendingCacheTime = datetime.now()
            self.orderPending = self.order_pending(self.market)
        if not self.isOrderUp():
            return False
        if self.orderPending['amount'] != self.orderPending['remaining']:
            return True
        return False

    def getCurrentOrderDetails(self, ignoreCache = False):
        if ( ignoreCache or 
            self.orderPendingCacheTime + timedelta(seconds=self.cacheOrderPendingSeconds) < datetime.now() ):
            self.orderPendingCacheTime = datetime.now()
            self.orderPending = self.order_pending(self.market)
        return self.orderPending

    def getLastOrderDetails(self, ignoreCache = False):
        if ( ignoreCache or 
            self.orderDoneCacheTime + timedelta(seconds=self.cacheOrderDoneSeconds) < datetime.now() ):
            self.orderDoneCacheTime = datetime.now()
            self.orderDone = self.order_finished(self.market)

        return self.orderDone


    def createOrder(self, botType, price, amount):
        self.orderPending = order.order(self.market, botType, price, amount, self.currentAction)

    def getAccountValue(self):
        USDest = 0.0
        USDest += self.toUSDRaw(self.available[self.sellCoin], self.sellCoin)
        print("Available {1:s} ${0:0.2f}".format(self.toUSDRaw(self.available[self.sellCoin], self.sellCoin), self.sellCoin))
        USDest += self.toUSDRaw(self.available[self.buyCoin], self.buyCoin)
        print("Available {1:s} ${0:0.2f}".format(self.toUSDRaw(self.available[self.buyCoin], self.buyCoin), self.buyCoin))
        USDest += self.toUSDRaw(self.frozen[self.sellCoin], self.sellCoin)
        print("Frozen {1:s} ${0:0.2f}".format(self.toUSDRaw(self.frozen[self.sellCoin], self.sellCoin), self.sellCoin))
        USDest += self.toUSDRaw(self.frozen[self.buyCoin], self.buyCoin)
        print("Frozen {1:s} ${0:0.2f}".format(self.toUSDRaw(self.frozen[self.buyCoin], self.buyCoin), self.buyCoin))
        USDest += self.toUSDRaw(self.available['CET'], 'CET')
        print("Available {1:s} ${0:0.2f}".format(self.toUSDRaw(self.available['CET'], 'CET'), 'CET'))
        return USDest

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


    def findDealMoney(self, price, amount):
        return amount * price

    def findAmount(self, price, dealMoney):
        return 0 if ( price == 0 ) else ( dealMoney / price )

    def findPrice(self, dealMoney, amount):
        return 0 if ( amount == 0 ) else ( dealMoney / amount )


    def get_account(self, buyCoin, sellCoin):
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


    def order_pending(self, market):
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

        
        orderPending = {
            'orderUp': False,
            'amount': 0,
            'remaining': 0,
            'price': 0,
            'id': 0
        }
        try:
            if latest['data']['count'] == 0:
                return orderPending

            self.orderPending['price'] = latest['data']['data'][0]['price']
            self.orderPending['amount'] = latest['data']['data'][0]['amount']
            self.orderPending['remaining'] = latest['data']['data'][0]['left']
            self.orderPending['id'] = latest['data']['data'][0]['id']
            self.orderPending['orderUp'] = True
        except IndexError:
            self.orderPending['orderUp'] = False
        except KeyError:
            self.orderPending['orderUp'] = False




    def order_finished(self, market):
        request_client = RequestClient()
        params = {
            'market': market,
            'page': 1,
            'limit': 1
        }
        response = request_client.request(
                'GET',
                '{url}/v1/order/finished'.format(url=request_client.url),
                params=params
        )
        latest = complex_json.loads(response.data)
        return latest['data']['data']


    def put_limit(self, data):
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


    def cancel_order(self, id, market):
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


