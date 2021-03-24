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
    elif tSecSlope < -9:
        string += 'L'
    elif tSecSlope < 0:
        string += 'C'
    elif tSecSlope < 9:
        string += 'Q'
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
    if price >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= tSecAvg:
        return 'PFTOS'
    if price >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= tSecAvg:
        return 'PFOTS'
    if price >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= tSecAvg:
        return 'PTFOS'
    if price >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= tSecAvg:
        return 'PTOFS'
    if price >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= tSecAvg:
        return 'POFTS'
    if price >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= tSecAvg:
        return 'POTFS'

    if fiveAvg >= price and price >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= tSecAvg:
        return 'FPTOS'
    if fiveAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= tSecAvg:
        return 'FPOTS'
    if fiveAvg >= thirtyAvg and thirtyAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= tSecAvg:
        return 'FTPOS'
    if fiveAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= tSecAvg:
        return 'FTOPS'
    if fiveAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= thirtyAvg and thirtyAvg >= tSecAvg:
        return 'FOPTS'
    if fiveAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= price and price >= tSecAvg:
        return 'FOTPS'

    if thirtyAvg >= price and price >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= tSecAvg:
        return 'TPFOS'
    if thirtyAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= tSecAvg:
        return 'TPOFS'
    if thirtyAvg >= fiveAvg and fiveAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= tSecAvg:
        return 'TFPOS'
    if thirtyAvg >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= tSecAvg:
        return 'TFOPS'
    if thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= fiveAvg and fiveAvg >= tSecAvg:
        return 'TOPFS'
    if thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= price and price >= tSecAvg:
        return 'TOFPS'

    if oneTwentyAvg >= price and price >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= tSecAvg:
        return 'OPFTS'
    if oneTwentyAvg >= price and price >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= tSecAvg:
        return 'OPTFS'
    if oneTwentyAvg >= fiveAvg and fiveAvg >= price and price >= thirtyAvg and thirtyAvg >= tSecAvg:
        return 'OFPTS'
    if oneTwentyAvg >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= price and price >= tSecAvg:
        return 'OFTPS'
    if oneTwentyAvg >= thirtyAvg and thirtyAvg >= price and price >= fiveAvg and fiveAvg >= tSecAvg:
        return 'OTPFS'
    if oneTwentyAvg >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= price and price >= tSecAvg:
        return 'OTFPS'

    if price >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= tSecAvg and tSecAvg >= oneTwentyAvg:
        return 'PFTSO'
    if price >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= tSecAvg and tSecAvg >= thirtyAvg:
        return 'PFOST'
    if price >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= tSecAvg and tSecAvg >= oneTwentyAvg:
        return 'PTFSO'
    if price >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= tSecAvg and tSecAvg >= fiveAvg:
        return 'PTOSF'
    if price >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= tSecAvg and tSecAvg >= thirtyAvg:
        return 'POFST'
    if price >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= tSecAvg and tSecAvg >= fiveAvg:
        return 'POTSF'

    if fiveAvg >= price and price >= thirtyAvg and thirtyAvg >= tSecAvg and tSecAvg >= oneTwentyAvg:
        return 'FPTSO'
    if fiveAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= tSecAvg and tSecAvg >= thirtyAvg:
        return 'FPOST'
    if fiveAvg >= thirtyAvg and thirtyAvg >= price and price >= tSecAvg and tSecAvg >= oneTwentyAvg:
        return 'FTPSO'
    if fiveAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= tSecAvg and tSecAvg >= price:
        return 'FTOSP'
    if fiveAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= tSecAvg and tSecAvg >= thirtyAvg:
        return 'FOPST'
    if fiveAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= tSecAvg and tSecAvg >= price:
        return 'FOTSP'

    if thirtyAvg >= price and price >= fiveAvg and fiveAvg >= tSecAvg and tSecAvg >= oneTwentyAvg:
        return 'TPFSO'
    if thirtyAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= tSecAvg and tSecAvg >= fiveAvg:
        return 'TPOSF'
    if thirtyAvg >= fiveAvg and fiveAvg >= price and price >= tSecAvg and tSecAvg >= oneTwentyAvg:
        return 'TFPSO'
    if thirtyAvg >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= tSecAvg and tSecAvg >= price:
        return 'TFOSP'
    if thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= tSecAvg and tSecAvg >= fiveAvg:
        return 'TOPSF'
    if thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= tSecAvg and tSecAvg >= price:
        return 'TOFSP'

    if oneTwentyAvg >= price and price >= fiveAvg and fiveAvg >= tSecAvg and tSecAvg >= thirtyAvg:
        return 'OPFST'
    if oneTwentyAvg >= price and price >= thirtyAvg and thirtyAvg >= tSecAvg and tSecAvg >= fiveAvg:
        return 'OPTSF'
    if oneTwentyAvg >= fiveAvg and fiveAvg >= price and price >= tSecAvg and tSecAvg >= thirtyAvg:
        return 'OFPST'
    if oneTwentyAvg >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= tSecAvg and tSecAvg >= price:
        return 'OFTSP'
    if oneTwentyAvg >= thirtyAvg and thirtyAvg >= price and price >= tSecAvg and tSecAvg >= fiveAvg:
        return 'OTPSF'
    if oneTwentyAvg >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= tSecAvg and tSecAvg >= price:
        return 'OTFSP'

    if price >= fiveAvg and fiveAvg >= tSecAvg and tSecAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg:
        return 'PFSTO'
    if price >= fiveAvg and fiveAvg >= tSecAvg and tSecAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg:
        return 'PFSOT'
    if price >= thirtyAvg and thirtyAvg >= tSecAvg and tSecAvg >= fiveAvg and fiveAvg >= oneTwentyAvg:
        return 'PTSFO'
    if price >= thirtyAvg and thirtyAvg >= tSecAvg and tSecAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg:
        return 'PTSOF'
    if price >= oneTwentyAvg and oneTwentyAvg >= tSecAvg and tSecAvg >= fiveAvg and fiveAvg >= thirtyAvg:
        return 'POSFT'
    if price >= oneTwentyAvg and oneTwentyAvg >= tSecAvg and tSecAvg >= thirtyAvg and thirtyAvg >= fiveAvg:
        return 'POSTF'

    if fiveAvg >= price and price >= tSecAvg and tSecAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg:
        return 'FPSTO'
    if fiveAvg >= price and price >= tSecAvg and tSecAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg:
        return 'FPSOT'
    if fiveAvg >= thirtyAvg and thirtyAvg >= tSecAvg and tSecAvg >= price and price >= oneTwentyAvg:
        return 'FTSPO'
    if fiveAvg >= thirtyAvg and thirtyAvg >= tSecAvg and tSecAvg >= oneTwentyAvg and oneTwentyAvg >= price:
        return 'FTSOP'
    if fiveAvg >= oneTwentyAvg and oneTwentyAvg >= tSecAvg and tSecAvg >= price and price >= thirtyAvg:
        return 'FOSPT'
    if fiveAvg >= oneTwentyAvg and oneTwentyAvg >= tSecAvg and tSecAvg >= thirtyAvg and thirtyAvg >= price:
        return 'FOSTP'

    if thirtyAvg >= price and price >= tSecAvg and tSecAvg >= fiveAvg and fiveAvg >= oneTwentyAvg:
        return 'TPSFO'
    if thirtyAvg >= price and price >= tSecAvg and tSecAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg:
        return 'TPSOF'
    if thirtyAvg >= fiveAvg and fiveAvg >= tSecAvg and tSecAvg >= price and price >= oneTwentyAvg:
        return 'TFSPO'
    if thirtyAvg >= fiveAvg and fiveAvg >= tSecAvg and tSecAvg >= oneTwentyAvg and oneTwentyAvg >= price:
        return 'TFSOP'
    if thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= tSecAvg and tSecAvg >= price and price >= fiveAvg:
        return 'TOSPF'
    if thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= tSecAvg and tSecAvg >= fiveAvg and fiveAvg >= price:
        return 'TOSFP'

    if oneTwentyAvg >= price and price >= tSecAvg and tSecAvg >= fiveAvg and fiveAvg >= thirtyAvg:
        return 'OPSFT'
    if oneTwentyAvg >= price and price >= tSecAvg and tSecAvg >= thirtyAvg and thirtyAvg >= fiveAvg:
        return 'OPSTF'
    if oneTwentyAvg >= fiveAvg and fiveAvg >= tSecAvg and tSecAvg >= price and price >= thirtyAvg:
        return 'OFSPT'
    if oneTwentyAvg >= fiveAvg and fiveAvg >= tSecAvg and tSecAvg >= thirtyAvg and thirtyAvg >= price:
        return 'OFSTP'
    if oneTwentyAvg >= thirtyAvg and thirtyAvg >= tSecAvg and tSecAvg >= price and price >= fiveAvg:
        return 'OTSPF'
    if oneTwentyAvg >= thirtyAvg and thirtyAvg >= tSecAvg and tSecAvg >= fiveAvg and fiveAvg >= price:
        return 'OTSFP'

    if price >= tSecAvg and tSecAvg >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg:
        return 'PSFTO'
    if price >= tSecAvg and tSecAvg >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg:
        return 'PSFOT'
    if price >= tSecAvg and tSecAvg >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= oneTwentyAvg:
        return 'PSTFO'
    if price >= tSecAvg and tSecAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg:
        return 'PSTOF'
    if price >= tSecAvg and tSecAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= thirtyAvg:
        return 'PSOFT'
    if price >= tSecAvg and tSecAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= fiveAvg:
        return 'PSOTF'

    if fiveAvg >= tSecAvg and tSecAvg >= price and price >= thirtyAvg and thirtyAvg >= oneTwentyAvg:
        return 'FSPTO'
    if fiveAvg >= tSecAvg and tSecAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg:
        return 'FSPOT'
    if fiveAvg >= tSecAvg and tSecAvg >= thirtyAvg and thirtyAvg >= price and price >= oneTwentyAvg:
        return 'FSTPO'
    if fiveAvg >= tSecAvg and tSecAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= price:
        return 'FSTOP'
    if fiveAvg >= tSecAvg and tSecAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= thirtyAvg:
        return 'FSOPT'
    if fiveAvg >= tSecAvg and tSecAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= price:
        return 'FSOTP'

    if thirtyAvg >= tSecAvg and tSecAvg >= price and price >= fiveAvg and fiveAvg >= oneTwentyAvg:
        return 'TSPFO'
    if thirtyAvg >= tSecAvg and tSecAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= fiveAvg:
        return 'TSPOF'
    if thirtyAvg >= tSecAvg and tSecAvg >= fiveAvg and fiveAvg >= price and price >= oneTwentyAvg:
        return 'TSFPO'
    if thirtyAvg >= tSecAvg and tSecAvg >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= price:
        return 'TSFOP'
    if thirtyAvg >= tSecAvg and tSecAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= fiveAvg:
        return 'TSOPF'
    if thirtyAvg >= tSecAvg and tSecAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= price:
        return 'TSOFP'

    if oneTwentyAvg >= tSecAvg and tSecAvg >= price and price >= fiveAvg and fiveAvg >= thirtyAvg:
        return 'OSPFT'
    if oneTwentyAvg >= tSecAvg and tSecAvg >= price and price >= thirtyAvg and thirtyAvg >= fiveAvg:
        return 'OSPTF'
    if oneTwentyAvg >= tSecAvg and tSecAvg >= fiveAvg and fiveAvg >= price and price >= thirtyAvg:
        return 'OSFPT'
    if oneTwentyAvg >= tSecAvg and tSecAvg >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= price:
        return 'OSFTP'
    if oneTwentyAvg >= tSecAvg and tSecAvg >= thirtyAvg and thirtyAvg >= price and price >= fiveAvg:
        return 'OSTPF'
    if oneTwentyAvg >= tSecAvg and tSecAvg >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= price:
        return 'OSTFP'

    if tSecAvg >= price and price >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg:
        return 'SPFTO'
    if tSecAvg >= price and price >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg:
        return 'SPFOT'
    if tSecAvg >= price and price >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= oneTwentyAvg:
        return 'SPTFO'
    if tSecAvg >= price and price >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg:
        return 'SPTOF'
    if tSecAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= thirtyAvg:
        return 'SPOFT'
    if tSecAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= fiveAvg:
        return 'SPOTF'

    if tSecAvg >= fiveAvg and fiveAvg >= price and price >= thirtyAvg and thirtyAvg >= oneTwentyAvg:
        return 'SFPTO'
    if tSecAvg >= fiveAvg and fiveAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg:
        return 'SFPOT'
    if tSecAvg >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= price and price >= oneTwentyAvg:
        return 'SFTPO'
    if tSecAvg >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= price:
        return 'SFTOP'
    if tSecAvg >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= thirtyAvg:
        return 'SFOPT'
    if tSecAvg >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= price:
        return 'SFOTP'

    if tSecAvg >= thirtyAvg and thirtyAvg >= price and price >= fiveAvg and fiveAvg >= oneTwentyAvg:
        return 'STPFO'
    if tSecAvg >= thirtyAvg and thirtyAvg >= price and price >= oneTwentyAvg and oneTwentyAvg >= fiveAvg:
        return 'STPOF'
    if tSecAvg >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= price and price >= oneTwentyAvg:
        return 'STFPO'
    if tSecAvg >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= oneTwentyAvg and oneTwentyAvg >= price:
        return 'STFOP'
    if tSecAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= fiveAvg:
        return 'STOPF'
    if tSecAvg >= thirtyAvg and thirtyAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= price:
        return 'STOFP'

    if tSecAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= fiveAvg and fiveAvg >= thirtyAvg:
        return 'SOPFT'
    if tSecAvg >= oneTwentyAvg and oneTwentyAvg >= price and price >= thirtyAvg and thirtyAvg >= fiveAvg:
        return 'SOPTF'
    if tSecAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= price and price >= thirtyAvg:
        return 'SOFPT'
    if tSecAvg >= oneTwentyAvg and oneTwentyAvg >= fiveAvg and fiveAvg >= thirtyAvg and thirtyAvg >= price:
        return 'SOFTP'
    if tSecAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= price and price >= fiveAvg:
        return 'SOTPF'
    if tSecAvg >= oneTwentyAvg and oneTwentyAvg >= thirtyAvg and thirtyAvg >= fiveAvg and fiveAvg >= price:
        return 'SOTFP'
    return 'WRNG'