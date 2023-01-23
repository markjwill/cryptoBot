#!/usr/bin/python

import sys
import importlib
import time

import marketProvider as mp
import accountProvider as ap

import logFormatter as logger

import settings as settings

if len(sys.argv) not 6:
  print("ERROR: 5 arguments required.")
  print("python3 cryptoBot.py <buyCoin> <sellCoin> <accountSource> <marketSource> <decisionEngine>")
  exit()


  buyCoin = sys.argv[1]
  sellCoin = sys.argv[2]

  accountSourceName = sys.argv[3]
  marketSourceName = sys.argv[4]
  decisionEngineName = sys.argv[5]

try:
  accountSourceLib = importlib.import_module(accountSourceName, package=None)
except:
  print("ERROR failed to import accountSource")
  exit()

try:
  marketSourceLib = importlib.import_module(marketSourceName, package=None)
except:
  print("ERROR failed to import marketSource")
  exit()

try:
  decisionEngineLib = importlib.import_module(decisionEngineName, package=None)
except:
  print("ERROR failed to import decisionEngine")
  exit()

marketProvider = mp.marketProvider(buyCoin, sellCoin, marketSourceLib.marketSource())

accountProvider = ap.accountProvider(buyCoin, sellCoin, accountSourceLib.accountSource())

decisionEngine = decisionEngineLib.decisionEngine(marketProvider, accountProvider)

accountProvider.checkOrderHistory()

while True:
  accountProvider.checkPendingOrder()
  marketProvider.getLatestData()
  decisionEngine.decide(marketProvider, accountProvider)
  logger.update(decisionEngine)
  time.sleep(marketProvider.delay)
