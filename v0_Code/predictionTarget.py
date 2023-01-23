#!/usr/bin/python

class predictionTarget:

    name = ''
    seconds = 0

    params = {
        "predictedPrice" : 0.0,
        "actualPrice" : None
    }

    def __init__(self, name, seconds):
        self.name = name
        self.seconds = seconds