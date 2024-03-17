import logging

# trade[0] price
# trade[1] amount
# trade[2] type
# trade[3] date_ms
# trade[4] trade_id
# trades[5] coinbasePrice
# trades[6] huobiPrice
# trades[7] binancePrice

class Features:

    APROXIMATE_RECORDS = 13000000

    TIME_PERIODS = {
        'tenSeconds'       : 10000,
        'thirtySeconds'    : 30000,
        'ninetySeconds'    : 90000,
        'fiveMinutes'      : 300000,
        'fifteenMinutes'   : 900000,
        'fortyFiveMinutes' : 2700000,
        'twoHours'         : 7200001
    }

    FUTURE_TIME_PERIODS = {
        'fiveSeconds'      : 5000,
        'tenSeconds'       : 10000,
        'thirtySeconds'    : 30000,
    }

    FUTURE_FEATURES = {
        'futureHighPrice'  : 0.0, #high price - pivot price
        'futureLowPrice'   : 0.0, #low price - pivot price
        'futurePrice'      : 0.0, #future price - pivot price 
    }

    MAX_PERIOD = 7200001
    MAX_PERIOD_NAME = 'twoHours'
    MAX_FUTURE_PERIOD = 30001
    MAX_FUTURE_PERIOD_NAME = 'thirtySeconds'

    PRICE_PERIOD_FEATURES = {

    }

    PREVIOUS_TRADE_COUNT = 100

    OFF_EXCHANGE_PERIOD_FEATURES = {
        'diffExchange'     : 0.0,  # difference in price from exchange
        'diffExchangeSq'   : 0.0,  # squared
        'diffExchangeCu'   : 0.0
    }

    RICH_PERIOD_FEATURES = {
        'avgByTradesPrice' : 0.0, #avg Price by number of trades - pivot price
        'highPrice'        : 0.0, #high price - pivot price
        'lowPrice'         : 0.0, #low price - pivot price
        'changeReal'       : 0.0, #start to end change price
        # 'changePercent'    : 0.0, #start to end change price percent
        'travelReal'       : 0.0, #low to high change in price
        # 'travelPercent'    : 0.0, #low to high change price percent
        'upVsDown'         : 0,   #sum of price increases as +1 and price decreases as -1
        'avgByVolumePrice' : 0.0, #avg Price by volume - pivot price
        'volume'           : 0.0, #sum of trade amounts
        # 'volumePrMinute'   : 0.0, #sum of trade amounts per minute
        # 'sellsPrMinute'    : 0.0, #number of sells per minute
        # 'sells'            : 0,   #number of sells
        # 'buysPrMinute'     : 0.0, #number of buys per minute
        # 'buys'             : 0,   #number of buys
        'buyVsSell'        : 0,   #sum of buys as +1 and sells as -1
        'buyVsSellVolume'  : 0.0, #sum of buys as +volume and sells as -volume
        # 'tradesPrMinute'   : 0.0, #number of trades per minute
    }

    SINGLE_PERIOD_FEATURES = {
        'tradeCount' : 0, #number of trades
    }

    FEATURE_INDEXES = {
        'exchange' : { 'price' : 0, 'volume' : 1,     'type' : 2,     'quantity' : True  },
        'coinbase' : { 'price' : 5, 'volume' : False, 'type' : False, 'quantity' : False },
        'huobi'    : { 'price' : 6, 'volume' : False, 'type' : False, 'quantity' : False },
        'binance'  : { 'price' : 7, 'volume' : False, 'type' : False, 'quantity' : False },
    }

    NON_PERIOD_FEATURES = {
        'secondsIntoDaySin'  : 0,
        'secondsIntoDayCos'  : 0,
        'minutesIntoWeekSin' : 0,
        'minutesIntoWeekCos' : 0,
        'hoursIntoMonthSin'   : 0,
        'hoursIntoMonthCos'   : 0,
        # 'hoursIntoYearSin'    : 0,
        # 'hoursIntoYearCos'    : 0,
        'volume'             : 0, # amount traded
        'type'               : 0  # 1 = buy, -1 = sell
    }

    DO_NOT_NORMALIZE = [
        'index',
        'type',
        'secondsIntoDaySin',
        'secondsIntoDayCos',
        'minutesIntoWeekSin',
        'minutesIntoWeekCos',
        'hoursIntoMonthSin',
        'hoursIntoMonthCos',
        # 'hourIntoYearSin',
        # 'hourIntoYearCos',
    ]

    COLUMNS = []
    PERIOD_FEATURES = {}
    featuresToNormalize = []
    csvFiles = {}
    starterPercents = {}

    def __init__(self):
        self.initPeriodFeatures()
        self.initColumns()
        self.initFeaturesToNormalize()
        # self.initCsvFiles()
        # self.setStarterPercents()

    # def setStarterPercents(self):
    #     for timeName, milliseconds in self.TIME_PERIODS.items():
    #         self.starterPercents[timeName] = milliseconds / self.MAX_PERIOD
    #     logging.debug('Starter Percents:')
    #     logging.debug(self.starterPercents)

    def initCsvFiles(self):
        if self.csvFiles:
            return
        if not self.PERIOD_FEATURES:
            self.initPeriodFeatures()

        self.csvFiles['noNormalize'] = []
        self.csvFiles['normalize'] = []

        self.csvFiles['noNormalize'] = [key for key \
            in self.NON_PERIOD_FEATURES.keys() \
            if key in self.DO_NOT_NORMALIZE]
        self.csvFiles['normalize'] = [key for key in \
            self.NON_PERIOD_FEATURES.keys() \
            if key not in self.DO_NOT_NORMALIZE]

        for timeName in self.TIME_PERIODS:
            for featureName in self.PERIOD_FEATURES:
                self.csvFiles['normalize'].append(f'{timeName}_{featureName}')
            self.csvFiles[timeName] = [f'{timeName}_futurePrice']

        for negativeIndex in range(1,self.PREVIOUS_TRADE_COUNT):
            key = f'trade-{negativeIndex}-price'
            self.csvFiles['normalize'].append(key)
            key = f'trade-{negativeIndex}-volume'
            self.csvFiles['normalize'].append(key)
            key = f'trade-{negativeIndex}-date_ms'
            self.csvFiles['normalize'].append(key)

        logging.debug(self.csvFiles)

    def initFeaturesToNormalize(self):
        # futurePriceFeatures = [f'{timeName}_futurePrice' for timeName in self.FUTURE_TIME_PERIODS.keys()]
        doNotNormalize = self.DO_NOT_NORMALIZE
        self.featuresToNormalize = [i for i in self.COLUMNS if i not in doNotNormalize]
        logging.debug('featuresToNormalize:')
        logging.debug(self.featuresToNormalize)

    def initPeriodFeatures(self):
        if self.PERIOD_FEATURES:
            return

        for featureName, default in self.SINGLE_PERIOD_FEATURES.items():
            self.PERIOD_FEATURES[featureName] = default

        for sourceName, index in self.FEATURE_INDEXES.items():
            if index['price'] is not False:
                for feature, default in self.PRICE_PERIOD_FEATURES.items():
                    self.PERIOD_FEATURES[f'{sourceName}_{feature}'] = default
            if    index['price'] is not False and \
                 index['volume'] is not False and \
                   index['type'] is not False and \
               index['quantity'] is not False:
                for feature, default in self.RICH_PERIOD_FEATURES.items():
                    self.PERIOD_FEATURES[f'{sourceName}_{feature}'] = default
            if sourceName != 'exchange':
                for feature, default in self.OFF_EXCHANGE_PERIOD_FEATURES.items():
                    self.PERIOD_FEATURES[f'{sourceName}_{feature}'] = default

        logging.debug('FEATURES:')
        logging.debug(self.PERIOD_FEATURES)

    def initColumns(self):
        if self.COLUMNS:
            return
        if not self.PERIOD_FEATURES:
            self.initPeriodFeatures()

        for featureName, default in self.NON_PERIOD_FEATURES.items():
            self.COLUMNS.append(featureName)

        for timeName in self.TIME_PERIODS:
            for featureName in self.PERIOD_FEATURES:
                self.COLUMNS.append(f'{timeName}_{featureName}')

        for timeName in self.FUTURE_TIME_PERIODS:
            for featureName in self.FUTURE_FEATURES:
                self.COLUMNS.append(f'{timeName}_{featureName}')

        for negativeIndex in range(1,self.PREVIOUS_TRADE_COUNT):
            self.COLUMNS.append(f'trade-{negativeIndex}-price')
            self.COLUMNS.append(f'trade-{negativeIndex}-volume')
            self.COLUMNS.append(f'trade-{negativeIndex}-date_ms')

        self.COLUMNS = sorted(self.COLUMNS, key=str.lower)
        logging.debug('COLUMNS:')
        logging.debug(self.COLUMNS) 
