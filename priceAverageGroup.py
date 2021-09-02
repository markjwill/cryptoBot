#!/usr/bin/python

class priceAverageGroup:

    name = ''
    seconds = 0

    params = {
        "avgPrice": 0.0,
        "minuteSlope": 0.0
        # highPrice = 0.0 #high price
        # lowPrice = 0.0 #low price
        # startPrice = 0.0 #start price
        # endPrice = 0.0 #end price, aka Current Price if realtime data
        # changeReal = 0.0 #start to end change price
        # changePercent = 0.0 #start to end change price percent
        # travelReal = 0.0 #max travel in price
        # travelPercent = 0.0 #max travel in price percent
        # volume = 0.0 #total volume
        # volumePrMin = 0.0 #
        # totalPrice = 0.0 #total
        # tradeCount = len(trades)
        # sellsPrMin = 0.0
        # sells = 0
        # buysPrMin = 0.0
        # buys = 0
    }

    def __init__(self, name, seconds):
        self.name = name
        self.seconds = seconds
