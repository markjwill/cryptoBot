import datetime
import numpy as np

def prepTrade(trade):
  # Time Features
  dt = datetime.datetime.fromtimestamp(trade[2])
  t = dt.time()
  secondsIntoDay = (t.hour * 60 + t.minute) * 60 + t.second
  dayIntoYear = dt.timetuple().tm_yday
  preped = {}
  preped[0] = trade[0] #trade_id
  preped[1] = np.sin(secondsIntoDay * (2 * np.pi / 86400)) # seconds_in_day_sin
  preped[2] = np.cos(secondsIntoDay * (2 * np.pi / 86400)) # seconds_in_day_cos
  preped[3] = np.sin(dt.weekday() * (2 * np.pi / 7)) # day_in_week_sin
  preped[4] = np.cos(dt.weekday() * (2 * np.pi / 7)) # day_in_week_cos
  preped[5] = np.sin(dayIntoYear * (2 * np.pi / 365)) # day_in_year_sin
  preped[6] = np.cos(dayIntoYear * (2 * np.pi / 365)) # day_in_year_cos
  preped[7] = trade[1] # amount_traded
  preped[8] = 1 if trade[4] == 'buy' else -1 # type
  preped[9] = trade[3] - trade[5] if trade[5] != 0 else 0 # p_fiveSec_avgPrice
  preped[10] = trade[3] - trade[6] if trade[6] != 0 else 0 # p_fiveSec_highPrice
  preped[11] = trade[3] - trade[7] if trade[7] != 0 else 0 # p_fiveSec_lowPrice
  preped[12] = trade[3] - trade[8] if trade[8] != 0 else 0 # p_fiveSec_startPrice
  preped[13] = trade[3] - trade[9] if trade[9] != 0 else 0 # p_fiveSec_endPrice
  preped[14] = trade[10] # p_fiveSec_changeReal
  preped[15] = trade[11] # p_fiveSec_changePercent
  preped[16] = trade[12] # p_fiveSec_travelReal
  preped[17] = trade[13] # p_fiveSec_travelPercent
  preped[18] = trade[14] # p_fiveSec_volumePrMin
  preped[19] = trade[15] # p_fiveSec_tradesPrMin
  preped[20] = trade[16] # p_fiveSec_buysPrMin
  preped[21] = trade[17] # p_fiveSec_sellsPrMin
  preped[22] = trade[3] - trade[18] if trade[18] != 0 else 0 # p_tenSec_avgPrice
  preped[23] = trade[3] - trade[19] if trade[19] != 0 else 0 # p_tenSec_highPrice
  preped[24] = trade[3] - trade[20] if trade[20] != 0 else 0 # p_tenSec_lowPrice
  preped[25] = trade[3] - trade[21] if trade[21] != 0 else 0 # p_tenSec_startPrice
  preped[26] = trade[3] - trade[22] if trade[22] != 0 else 0 # p_tenSec_endPrice
  preped[27] = trade[23] # p_tenSec_changeReal
  preped[28] = trade[24] # p_tenSec_changePercent
  preped[29] = trade[25] # p_tenSec_travelReal
  preped[30] = trade[26] # p_tenSec_travelPercent
  preped[31] = trade[27] # p_tenSec_volumePrMin
  preped[32] = trade[28] # p_tenSec_tradesPrMin
  preped[33] = trade[29] # p_tenSec_buysPrMin
  preped[34] = trade[30] # p_tenSec_sellsPrMin
  preped[35] = trade[3] - trade[31] if trade[31] != 0 else 0 # p_thirtySec_avgPrice
  preped[36] = trade[3] - trade[32] if trade[32] != 0 else 0 # p_thirtySec_highPrice
  preped[37] = trade[3] - trade[33] if trade[33] != 0 else 0 # p_thirtySec_lowPrice
  preped[38] = trade[3] - trade[34] if trade[34] != 0 else 0 # p_thirtySec_startPrice
  preped[39] = trade[3] - trade[35] if trade[35] != 0 else 0 # p_thirtySec_endPrice
  preped[40] = trade[36] # p_thirtySec_changeReal
  preped[41] = trade[37] # p_thirtySec_changePercent
  preped[42] = trade[38] # p_thirtySec_travelReal
  preped[43] = trade[39] # p_thirtySec_travelPercent
  preped[44] = trade[40] # p_thirtySec_volumePrMin
  preped[45] = trade[41] # p_thirtySec_tradesPrMin
  preped[46] = trade[42] # p_thirtySec_buysPrMin
  preped[47] = trade[43] # p_thirtySec_sellsPrMin
  preped[48] = trade[3] - trade[44] if trade[44] != 0 else 0 # p_oneMin_avgPrice
  preped[49] = trade[3] - trade[45] if trade[45] != 0 else 0 # p_oneMin_highPrice
  preped[50] = trade[3] - trade[46] if trade[46] != 0 else 0 # p_oneMin_lowPrice
  preped[51] = trade[3] - trade[47] if trade[47] != 0 else 0 # p_oneMin_startPrice
  preped[52] = trade[3] - trade[48] if trade[48] != 0 else 0 # p_oneMin_endPrice
  preped[53] = trade[49] # p_oneMin_changeReal
  preped[54] = trade[50] # p_oneMin_changePercent
  preped[55] = trade[51] # p_oneMin_travelReal
  preped[56] = trade[52] # p_oneMin_travelPercent
  preped[57] = trade[53] # p_oneMin_volumePrMin
  preped[58] = trade[54] # p_oneMin_tradesPrMin
  preped[59] = trade[55] # p_oneMin_buysPrMin
  preped[60] = trade[56] # p_oneMin_sellsPrMin
  preped[61] = trade[3] - trade[57] if trade[57] != 0 else 0 # p_threeMin_avgPrice
  preped[62] = trade[3] - trade[58] if trade[58] != 0 else 0 # p_threeMin_highPrice
  preped[63] = trade[3] - trade[59] if trade[59] != 0 else 0 # p_threeMin_lowPrice
  preped[64] = trade[3] - trade[60] if trade[60] != 0 else 0 # p_threeMin_startPrice
  preped[65] = trade[3] - trade[61] if trade[61] != 0 else 0 # p_threeMin_endPrice
  preped[66] = trade[62] # p_threeMin_changeReal
  preped[67] = trade[63] # p_threeMin_changePercent
  preped[68] = trade[64] # p_threeMin_travelReal
  preped[69] = trade[65] # p_threeMin_travelPercent
  preped[70] = trade[66] # p_threeMin_volumePrMin
  preped[71] = trade[67] # p_threeMin_tradesPrMin
  preped[72] = trade[68] # p_threeMin_buysPrMin
  preped[73] = trade[69] # p_threeMin_sellsPrMin
  preped[74] = trade[3] - trade[70] if trade[70] != 0 else 0 # p_fiveMin_avgPrice
  preped[75] = trade[3] - trade[71] if trade[71] != 0 else 0 # p_fiveMin_highPrice
  preped[76] = trade[3] - trade[72] if trade[72] != 0 else 0 # p_fiveMin_lowPrice
  preped[77] = trade[3] - trade[73] if trade[73] != 0 else 0 # p_fiveMin_startPrice
  preped[78] = trade[3] - trade[74] if trade[74] != 0 else 0 # p_fiveMin_endPrice
  preped[79] = trade[75] # p_fiveMin_changeReal
  preped[80] = trade[76] # p_fiveMin_changePercent
  preped[81] = trade[77] # p_fiveMin_travelReal
  preped[82] = trade[78] # p_fiveMin_travelPercent
  preped[83] = trade[79] # p_fiveMin_volumePrMin
  preped[84] = trade[80] # p_fiveMin_tradesPrMin
  preped[85] = trade[81] # p_fiveMin_buysPrMin
  preped[86] = trade[82] # p_fiveMin_sellsPrMin
  preped[87] = trade[3] - trade[83] if trade[83] != 0 else 0 # p_tenMin_avgPrice
  preped[88] = trade[3] - trade[84] if trade[84] != 0 else 0 # p_tenMin_highPrice
  preped[89] = trade[3] - trade[85] if trade[85] != 0 else 0 # p_tenMin_lowPrice
  preped[90] = trade[3] - trade[86] if trade[86] != 0 else 0 # p_tenMin_startPrice
  preped[91] = trade[3] - trade[87] if trade[87] != 0 else 0 # p_tenMin_endPrice
  preped[92] = trade[88] # p_tenMin_changeReal
  preped[93] = trade[89] # p_tenMin_changePercent
  preped[94] = trade[90] # p_tenMin_travelReal
  preped[95] = trade[91] # p_tenMin_travelPercent
  preped[96] = trade[92] # p_tenMin_volumePrMin
  preped[97] = trade[93] # p_tenMin_tradesPrMin
  preped[98] = trade[94] # p_tenMin_buysPrMin
  preped[99] = trade[95] # p_tenMin_sellsPrMin
  preped[100] = trade[3] - trade[96] if trade[96] != 0 else 0 # p_fifteenMin_avgPrice
  preped[101] = trade[3] - trade[97] if trade[97] != 0 else 0 # p_fifteenMin_highPrice
  preped[102] = trade[3] - trade[98] if trade[98] != 0 else 0 # p_fifteenMin_lowPrice
  preped[103] = trade[3] - trade[99] if trade[99] != 0 else 0 # p_fifteenMin_startPrice
  preped[104] = trade[3] - trade[100] if trade[100] != 0 else 0 # p_fifteenMin_endPrice
  preped[105] = trade[101] # p_fifteenMin_changeReal
  preped[106] = trade[102] # p_fifteenMin_changePercent
  preped[107] = trade[103] # p_fifteenMin_travelReal
  preped[108] = trade[104] # p_fifteenMin_travelPercent
  preped[109] = trade[105] # p_fifteenMin_volumePrMin
  preped[110] = trade[106] # p_fifteenMin_tradesPrMin
  preped[111] = trade[107] # p_fifteenMin_buysPrMin
  preped[112] = trade[108] # p_fifteenMin_sellsPrMin
  preped[113] = trade[3] - trade[109] if trade[109] != 0 else 0 # p_thirtyMin_avgPrice
  preped[114] = trade[3] - trade[110] if trade[110] != 0 else 0 # p_thirtyMin_highPrice
  preped[115] = trade[3] - trade[111] if trade[111] != 0 else 0 # p_thirtyMin_lowPrice
  preped[116] = trade[3] - trade[112] if trade[112] != 0 else 0 # p_thirtyMin_startPrice
  preped[117] = trade[3] - trade[113] if trade[113] != 0 else 0 # p_thirtyMin_endPrice
  preped[118] = trade[114] # p_thirtyMin_changeReal
  preped[119] = trade[115] # p_thirtyMin_changePercent
  preped[120] = trade[116] # p_thirtyMin_travelReal
  preped[121] = trade[117] # p_thirtyMin_travelPercent
  preped[122] = trade[118] # p_thirtyMin_volumePrMin
  preped[123] = trade[119] # p_thirtyMin_tradesPrMin
  preped[124] = trade[120] # p_thirtyMin_buysPrMin
  preped[125] = trade[121] # p_thirtyMin_sellsPrMin
  preped[126] = trade[3] - trade[122] if trade[122] != 0 else 0 # p_sixtyMin_avgPrice
  preped[127] = trade[3] - trade[123] if trade[123] != 0 else 0 # p_sixtyMin_highPrice
  preped[128] = trade[3] - trade[124] if trade[124] != 0 else 0 # p_sixtyMin_lowPrice
  preped[129] = trade[3] - trade[125] if trade[125] != 0 else 0 # p_sixtyMin_startPrice
  preped[130] = trade[3] - trade[126] if trade[126] != 0 else 0 # p_sixtyMin_endPrice
  preped[131] = trade[127] # p_sixtyMin_changeReal
  preped[132] = trade[128] # p_sixtyMin_changePercent
  preped[133] = trade[129] # p_sixtyMin_travelReal
  preped[134] = trade[130] # p_sixtyMin_travelPercent
  preped[135] = trade[131] # p_sixtyMin_volumePrMin
  preped[136] = trade[132] # p_sixtyMin_tradesPrMin
  preped[137] = trade[133] # p_sixtyMin_buysPrMin
  preped[138] = trade[134] # p_sixtyMin_sellsPrMin
  preped[139] = trade[3] - trade[135] if trade[135] != 0 else 0 # p_oneTwentyMin_avgPrice
  preped[140] = trade[3] - trade[136] if trade[136] != 0 else 0 # p_oneTwentyMin_highPrice
  preped[141] = trade[3] - trade[137] if trade[137] != 0 else 0 # p_oneTwentyMin_lowPrice
  preped[142] = trade[3] - trade[138] if trade[138] != 0 else 0 # p_oneTwentyMin_startPrice
  preped[143] = trade[3] - trade[139] if trade[139] != 0 else 0 # p_oneTwentyMin_endPrice
  preped[144] = trade[140] # p_oneTwentyMin_changeReal
  preped[145] = trade[141] # p_oneTwentyMin_changePercent
  preped[146] = trade[142] # p_oneTwentyMin_travelReal
  preped[147] = trade[143] # p_oneTwentyMin_travelPercent
  preped[148] = trade[144] # p_oneTwentyMin_volumePrMin
  preped[149] = trade[145] # p_oneTwentyMin_tradesPrMin
  preped[150] = trade[146] # p_oneTwentyMin_buysPrMin
  preped[151] = trade[147] # p_oneTwentyMin_sellsPrMin
  preped[152] = trade[3] - trade[148] if trade[148] != 0 else 0 # coinbase_price
  preped[153] = trade[3] - trade[149] if trade[149] != 0 else 0 # binance_price
  preped[154] = trade[3] - trade[150] if trade[150] != 0 else 0 # huobi_price
  preped[155] = trade[3] - trade[151] if trade[151] != 0 else 0 # bittrex_price
  preped[156] = trade[152] # bch_price
  preped[157] = trade[153] # eth_price
  preped[158] = trade[3] - trade[154] if trade[154] != 0 else 0 # f_fiveSec_endPrice
  preped[159] = trade[3] - trade[155] if trade[155] != 0 else 0 # f_thirtySec_endPrice
  preped[160] = trade[3] - trade[156] if trade[156] != 0 else 0 # f_fifteenMin_endPrice
  preped[161] = trade[3] - trade[157] if trade[157] != 0 else 0 # f_oneTwentyMin_endPrice
  return preped

def unprepY(preppedTrade, originalTrade):
  trade = np.zeros(162);
  trade[158] = originalTrade[3] + preppedTrade[158] if preppedTrade[158] != 0 else originalTrade[3] # f_fiveSec_endPrice
  trade[159] = originalTrade[3] + preppedTrade[159] if preppedTrade[159] != 0 else originalTrade[3] # f_thirtySec_endPrice
  trade[160] = originalTrade[3] + preppedTrade[160] if preppedTrade[160] != 0 else originalTrade[3] # f_fifteenMin_endPrice
  trade[161] = originalTrade[3] + preppedTrade[161] if preppedTrade[161] != 0 else originalTrade[3] # f_oneTwentyMin_endPrice
  return trade

""" 
CREATE TABLE prepedTrades (
  trade_id bigint(20) NOT NULL, 
  seconds_in_day_sin float NOT NULL, 
  seconds_in_day_cos float NOT NULL, 
  day_in_week_sin float NOT NULL, 
  day_in_week_cos float NOT NULL, 
  day_in_year_sin float NOT NULL, 
  day_in_year_cos float NOT NULL, 
  amount_traded float NOT NULL, 
  type float NOT NULL, 
  p_fiveSec_avgPrice float NOT NULL, 
  p_fiveSec_highPrice float NOT NULL, 
  p_fiveSec_lowPrice float NOT NULL, 
  p_fiveSec_startPrice float NOT NULL, 
  p_fiveSec_endPrice float NOT NULL, 
  p_fiveSec_changeReal float NOT NULL, 
  p_fiveSec_changePercent float NOT NULL, 
  p_fiveSec_travelReal float NOT NULL, 
  p_fiveSec_travelPercent float NOT NULL, 
  p_fiveSec_volumePrMin float NOT NULL, 
  p_fiveSec_tradesPrMin float NOT NULL, 
  p_fiveSec_buysPrMin float NOT NULL, 
  p_fiveSec_sellsPrMin float NOT NULL, 
  p_tenSec_avgPrice float NOT NULL, 
  p_tenSec_highPrice float NOT NULL, 
  p_tenSec_lowPrice float NOT NULL, 
  p_tenSec_startPrice float NOT NULL, 
  p_tenSec_endPrice float NOT NULL, 
  p_tenSec_changeReal float NOT NULL, 
  p_tenSec_changePercent float NOT NULL, 
  p_tenSec_travelReal float NOT NULL, 
  p_tenSec_travelPercent float NOT NULL, 
  p_tenSec_volumePrMin float NOT NULL, 
  p_tenSec_tradesPrMin float NOT NULL, 
  p_tenSec_buysPrMin float NOT NULL, 
  p_tenSec_sellsPrMin float NOT NULL, 
  p_thirtySec_avgPrice float NOT NULL, 
  p_thirtySec_highPrice float NOT NULL, 
  p_thirtySec_lowPrice float NOT NULL, 
  p_thirtySec_startPrice float NOT NULL, 
  p_thirtySec_endPrice float NOT NULL, 
  p_thirtySec_changeReal float NOT NULL, 
  p_thirtySec_changePercent float NOT NULL, 
  p_thirtySec_travelReal float NOT NULL, 
  p_thirtySec_travelPercent float NOT NULL, 
  p_thirtySec_volumePrMin float NOT NULL, 
  p_thirtySec_tradesPrMin float NOT NULL, 
  p_thirtySec_buysPrMin float NOT NULL, 
  p_thirtySec_sellsPrMin float NOT NULL, 
  p_oneMin_avgPrice float NOT NULL, 
  p_oneMin_highPrice float NOT NULL, 
  p_oneMin_lowPrice float NOT NULL, 
  p_oneMin_startPrice float NOT NULL, 
  p_oneMin_endPrice float NOT NULL, 
  p_oneMin_changeReal float NOT NULL, 
  p_oneMin_changePercent float NOT NULL, 
  p_oneMin_travelReal float NOT NULL, 
  p_oneMin_travelPercent float NOT NULL, 
  p_oneMin_volumePrMin float NOT NULL, 
  p_oneMin_tradesPrMin float NOT NULL, 
  p_oneMin_buysPrMin float NOT NULL, 
  p_oneMin_sellsPrMin float NOT NULL, 
  p_threeMin_avgPrice float NOT NULL, 
  p_threeMin_highPrice float NOT NULL, 
  p_threeMin_lowPrice float NOT NULL, 
  p_threeMin_startPrice float NOT NULL, 
  p_threeMin_endPrice float NOT NULL, 
  p_threeMin_changeReal float NOT NULL, 
  p_threeMin_changePercent float NOT NULL, 
  p_threeMin_travelReal float NOT NULL, 
  p_threeMin_travelPercent float NOT NULL, 
  p_threeMin_volumePrMin float NOT NULL, 
  p_threeMin_tradesPrMin float NOT NULL, 
  p_threeMin_buysPrMin float NOT NULL, 
  p_threeMin_sellsPrMin float NOT NULL, 
  p_fiveMin_avgPrice float NOT NULL, 
  p_fiveMin_highPrice float NOT NULL, 
  p_fiveMin_lowPrice float NOT NULL, 
  p_fiveMin_startPrice float NOT NULL, 
  p_fiveMin_endPrice float NOT NULL, 
  p_fiveMin_changeReal float NOT NULL, 
  p_fiveMin_changePercent float NOT NULL, 
  p_fiveMin_travelReal float NOT NULL, 
  p_fiveMin_travelPercent float NOT NULL, 
  p_fiveMin_volumePrMin float NOT NULL, 
  p_fiveMin_tradesPrMin float NOT NULL, 
  p_fiveMin_buysPrMin float NOT NULL, 
  p_fiveMin_sellsPrMin float NOT NULL, 
  p_tenMin_avgPrice float NOT NULL, 
  p_tenMin_highPrice float NOT NULL, 
  p_tenMin_lowPrice float NOT NULL, 
  p_tenMin_startPrice float NOT NULL, 
  p_tenMin_endPrice float NOT NULL, 
  p_tenMin_changeReal float NOT NULL, 
  p_tenMin_changePercent float NOT NULL, 
  p_tenMin_travelReal float NOT NULL, 
  p_tenMin_travelPercent float NOT NULL, 
  p_tenMin_volumePrMin float NOT NULL, 
  p_tenMin_tradesPrMin float NOT NULL, 
  p_tenMin_buysPrMin float NOT NULL, 
  p_tenMin_sellsPrMin float NOT NULL, 
  p_fifteenMin_avgPrice float NOT NULL, 
  p_fifteenMin_highPrice float NOT NULL, 
  p_fifteenMin_lowPrice float NOT NULL, 
  p_fifteenMin_startPrice float NOT NULL, 
  p_fifteenMin_endPrice float NOT NULL, 
  p_fifteenMin_changeReal float NOT NULL, 
  p_fifteenMin_changePercent float NOT NULL, 
  p_fifteenMin_travelReal float NOT NULL, 
  p_fifteenMin_travelPercent float NOT NULL, 
  p_fifteenMin_volumePrMin float NOT NULL, 
  p_fifteenMin_tradesPrMin float NOT NULL, 
  p_fifteenMin_buysPrMin float NOT NULL, 
  p_fifteenMin_sellsPrMin float NOT NULL, 
  p_thirtyMin_avgPrice float NOT NULL, 
  p_thirtyMin_highPrice float NOT NULL, 
  p_thirtyMin_lowPrice float NOT NULL, 
  p_thirtyMin_startPrice float NOT NULL, 
  p_thirtyMin_endPrice float NOT NULL, 
  p_thirtyMin_changeReal float NOT NULL, 
  p_thirtyMin_changePercent float NOT NULL, 
  p_thirtyMin_travelReal float NOT NULL, 
  p_thirtyMin_travelPercent float NOT NULL, 
  p_thirtyMin_volumePrMin float NOT NULL, 
  p_thirtyMin_tradesPrMin float NOT NULL, 
  p_thirtyMin_buysPrMin float NOT NULL, 
  p_thirtyMin_sellsPrMin float NOT NULL, 
  p_sixtyMin_avgPrice float NOT NULL, 
  p_sixtyMin_highPrice float NOT NULL, 
  p_sixtyMin_lowPrice float NOT NULL, 
  p_sixtyMin_startPrice float NOT NULL, 
  p_sixtyMin_endPrice float NOT NULL, 
  p_sixtyMin_changeReal float NOT NULL, 
  p_sixtyMin_changePercent float NOT NULL, 
  p_sixtyMin_travelReal float NOT NULL, 
  p_sixtyMin_travelPercent float NOT NULL, 
  p_sixtyMin_volumePrMin float NOT NULL, 
  p_sixtyMin_tradesPrMin float NOT NULL, 
  p_sixtyMin_buysPrMin float NOT NULL, 
  p_sixtyMin_sellsPrMin float NOT NULL, 
  p_oneTwentyMin_avgPrice float NOT NULL, 
  p_oneTwentyMin_highPrice float NOT NULL, 
  p_oneTwentyMin_lowPrice float NOT NULL, 
  p_oneTwentyMin_startPrice float NOT NULL, 
  p_oneTwentyMin_endPrice float NOT NULL, 
  p_oneTwentyMin_changeReal float NOT NULL, 
  p_oneTwentyMin_changePercent float NOT NULL, 
  p_oneTwentyMin_travelReal float NOT NULL, 
  p_oneTwentyMin_travelPercent float NOT NULL, 
  p_oneTwentyMin_volumePrMin float NOT NULL, 
  p_oneTwentyMin_tradesPrMin float NOT NULL, 
  p_oneTwentyMin_buysPrMin float NOT NULL, 
  p_oneTwentyMin_sellsPrMin float NOT NULL, 
  coinbase_price float NOT NULL, 
  binance_price float NOT NULL, 
  huobi_price float NOT NULL, 
  bittrex_price float NOT NULL, 
  bch_price float NOT NULL, 
  eth_price float NOT NULL, 
  f_fiveSec_endPrice float NOT NULL, 
  f_thirtySec_endPrice float NOT NULL, 
  f_fifteenMin_endPrice float NOT NULL, 
  f_oneTwentyMin_endPrice float NOT NULL
);
"""
