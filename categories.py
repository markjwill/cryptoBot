#!/usr/bin/python

# Shared category functions

def getSlopeString(tSecSlope, fiveSlope, thirtySlope, oneTwentySlope):
    string = ''

    # D = Down a lot
    # L = Down a little
    # C = Near 0 but less than 0
    # Q = Near 0 but more than 0
    # H = Up a little
    # U = Up a lot

    if tSecSlope < -28:
        string += 'D'
    # elif tSecSlope < -10:
    #     string += 'L'
    elif tSecSlope < -5:
        string += 'C'
    # elif tSecSlope < -2:
    #     string += 'K'
    elif tSecSlope < 0:
        string += 'Q'
    # elif tSecSlope < 2:
    #     string += 'G'
    elif tSecSlope < 5:
        string += 'Y'
    # elif tSecSlope < 10:
    #     string += 'Z'
    elif tSecSlope < 28:
        string += 'H'
    else:
        string += 'U'

    if fiveSlope < -10:
        string += 'D'
    elif fiveSlope < -4:
        string += 'L'
    elif fiveSlope < 0:
        string += 'C'
    elif fiveSlope < 4:
        string += 'Q'
    elif fiveSlope < 10:
        string += 'H'
    else:
        string += 'U'

    if thirtySlope < -5:
        string += 'D'
    elif thirtySlope < -1.5:
        string += 'L'
    elif thirtySlope < 0:
        string += 'C'
    elif thirtySlope < 1.5:
        string += 'Q'
    elif thirtySlope < 5:
        string += 'H'
    else:
        string += 'U'

    if oneTwentySlope < -2:
        string += 'D'
    elif oneTwentySlope < -0.75:
        string += 'L'
    elif oneTwentySlope < 0:
        string += 'C'
    elif oneTwentySlope < 0.75:
        string += 'Q'
    elif oneTwentySlope < 2:
        string += 'H'
    else:
        string += 'U'

    return string

def getRollingString(price, tSecAvg, fiveAvg, thirtyAvg, oneTwentyAvg):
    data = { 'P': price, 'S': tSecAvg, 'F': fiveAvg, 'T': thirtyAvg, 'O': oneTwentyAvg}
    values = sorted(data, key=data.__getitem__)
    string = ''
    for k in values[::-1]:
        string += k

    return string
