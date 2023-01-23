#!/usr/bin/python

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

urllib3.disable_warnings(InsecureRequestWarning)
http = urllib3.PoolManager(timeout=urllib3.Timeout(connect=1, read=2))

class accountSourceCoinEx(object):
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

    def getExchangeCoinMin():
        return 10300.0

    def getExchangeCoinSymbol():
        return 'CET'

    def getFee(self):
        return 0.00112

    def getFeeAsPercent(self):
        return 0.14

    def get_account(self, buyCoin, sellCoin):
        available = {}
        frozen = {}
        request_client = RequestClient()
        response = request_client.request('GET', '{url}/v1/balance/info'.format(url=request_client.url))
        latest = complex_json.loads(response.data)
        if sellCoin in latest['data']:
            available[sellCoin] = float(latest['data'][sellCoin]['available'])
            frozen[sellCoin] = float(latest['data'][sellCoin]['frozen'])
            # print(sellCoin + " Balance Found: ", Bot.available[sellCoin])
        else:
            available[sellCoin] = 0.0
        if buyCoin in latest['data']:
            available[buyCoin] = float(latest['data'][buyCoin]['available'])
            frozen[buyCoin] = float(latest['data'][buyCoin]['frozen'])
            # print(buyCoin + " Balance Found: ", Bot.available[buyCoin])
        else:
            available[buyCoin] = 0.0
        if 'CET' in latest['data']:
            available['CET'] = float(latest['data']['CET']['available'])
        else:
            available['CET'] = 0.0
        # print(complex_json.dumps(latest['data'], indent = 4, sort_keys=True))
        return available, frozen

    def order_status(self, market, tradeId):
        request_client = RequestClient()
        params = {
            'market': market,
            'id': tradeId
        }
        response = request_client.request(
                'GET',
                '{url}/v1/order/status'.format(url=request_client.url),
                params=params
        )

        try:
            latest = complex_json.loads(response.data)
        except AttributeError:
            print("Internet connectivity error, order_pending", flush=True)
            exit()
        return latest['data']
# Response
# {
#   "code": 0,
#   "data": {                           # order data
#     "amount": "1000",                 # order count
#     "avg_price": "11782.28",          # average executed price
#     "create_time": 1496761108,        # order placing time
#     "deal_amount": "1000",            # executed amount
#     "deal_fee": "23564.5798468",      # transaction fee
#     "deal_money": "11782289.9234",    # executed value
#     "id": 300021,                     # order no.
#     "left": "9.4",                    # unexecuted amount
#     "maker_fee_rate": "0.001",        # maker fee
#     "market": "BTCBCH",               # market
#     "order_type": "limit",            # order type
#     "price": "7000",                  # order price
#     "status": "done",                 # order status
#     "taker_fee_rate": "0.002",        # taker fee
#     "type": "sell"                    # order type
#     }
#   },
#   "message": "Ok"
# }


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
        return latest['data']['data'][0]
# Response
# {
#   "code": 0,
#   "data": {
#     "count": 1,                   # current page rows
#     "curr_page": 1,               # current page
#     "data": [                     # return in reverse of order time with latest order on top
#       {
#         "amount": "1.00",         # order count 
#         "avg_price": "0.00",      # average order price
#         "create_time": 1494320533,# order placing time
#         "deal_amount": "0.001",   # executed amount
#         "deal_fee": "130.3792",   # transaction fee
#         "deal_money": "65189.6",  # executed value
#         "id": 32,                 # order no.
#         "left": 32,               # unexecuted amount
#         "maker_fee_rate": "0",    # maker fee
#         "market": "BTCBCH",       # market type
#         "order_type": "limit",    # order type
#         "price": "10.00",         # order price
#         "status": "not_deal",     # order status
#         "taker_fee_rate": "0.002",# taker fee
#         "type": "sell",           # buy/sell type
#         "client_id": "xxxxx",     # order client id
#       }
#     ],
#     "has_next": true              # Is there a next page
#   },
#   "message": "Ok"
# }



    def order_finished(self, market, page, limit, trader):
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
# name           # type    description
# amount         # String  order count
# avg_price      # String  average price
# create_time    # Interger    time when placing order
# deal_amount    # String  count
# deal_fee       # String  transaction fee
# deal_money     # String  executed value
# finished_time  # Interger    complete time
# id             # Interger    Order No.
# maker_fee_rate # String  maker fee
# market         # String  See <API invocation description·market>
# order_type     # String  limit:limit order; market:market order;
# price          # String  order price
# status         # String  not_deal: unexecuted;
#                          part_deal: partly executed;
#                          done: executed;
# taker_fee_rate # String  taker fee
# type           # String  sell: sell order;
#                          buy: buy order;
# client_id      # String  order client id
        if latest['data']['count'] == 0:
            return []
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
        return latest['data']['data']


