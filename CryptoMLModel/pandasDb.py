import mariadb
import credentials
import pandas as pd
import numpy as np
import pymysql

def connect():
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
  return conn


def remove_outliers(df,columns,n_std):
    for col in columns:
        print('Working on column: {}'.format(col))
        
        mean = df[col].mean()
        sd = df[col].std()
        
        df = df[(df[col] <= mean+(n_std*sd))]
        
    return df

selectCols = "trade_id, seconds_in_day_sin, seconds_in_day_cos, day_in_week_sin, day_in_week_cos, day_in_year_sin, day_in_year_cos, amount_traded, type, p_fiveSec_avgPrice, p_fiveSec_highPrice, p_fiveSec_lowPrice, p_fiveSec_startPrice, p_fiveSec_endPrice, p_fiveSec_changeReal, p_fiveSec_changePercent, p_fiveSec_travelReal, p_fiveSec_travelPercent, p_fiveSec_volumePrMin, p_fiveSec_tradesPrMin, p_fiveSec_buysPrMin, p_fiveSec_sellsPrMin, p_tenSec_avgPrice, p_tenSec_highPrice, p_tenSec_lowPrice, p_tenSec_startPrice, p_tenSec_endPrice, p_tenSec_changeReal, p_tenSec_changePercent, p_tenSec_travelReal, p_tenSec_travelPercent, p_tenSec_volumePrMin, p_tenSec_tradesPrMin, p_tenSec_buysPrMin, p_tenSec_sellsPrMin, p_thirtySec_avgPrice, p_thirtySec_highPrice, p_thirtySec_lowPrice, p_thirtySec_startPrice, p_thirtySec_endPrice, p_thirtySec_changeReal, p_thirtySec_changePercent, p_thirtySec_travelReal, p_thirtySec_travelPercent, p_thirtySec_volumePrMin, p_thirtySec_tradesPrMin, p_thirtySec_buysPrMin, p_thirtySec_sellsPrMin, p_oneMin_avgPrice, p_oneMin_highPrice, p_oneMin_lowPrice, p_oneMin_startPrice, p_oneMin_endPrice, p_oneMin_changeReal, p_oneMin_changePercent, p_oneMin_travelReal, p_oneMin_travelPercent, p_oneMin_volumePrMin, p_oneMin_tradesPrMin, p_oneMin_buysPrMin, p_oneMin_sellsPrMin, p_threeMin_avgPrice, p_threeMin_highPrice, p_threeMin_lowPrice, p_threeMin_startPrice, p_threeMin_endPrice, p_threeMin_changeReal, p_threeMin_changePercent, p_threeMin_travelReal, p_threeMin_travelPercent, p_threeMin_volumePrMin, p_threeMin_tradesPrMin, p_threeMin_buysPrMin, p_threeMin_sellsPrMin, p_fiveMin_avgPrice, p_fiveMin_highPrice, p_fiveMin_lowPrice, p_fiveMin_startPrice, p_fiveMin_endPrice, p_fiveMin_changeReal, p_fiveMin_changePercent, p_fiveMin_travelReal, p_fiveMin_travelPercent, p_fiveMin_volumePrMin, p_fiveMin_tradesPrMin, p_fiveMin_buysPrMin, p_fiveMin_sellsPrMin, p_tenMin_avgPrice, p_tenMin_highPrice, p_tenMin_lowPrice, p_tenMin_startPrice, p_tenMin_endPrice, p_tenMin_changeReal, p_tenMin_changePercent, p_tenMin_travelReal, p_tenMin_travelPercent, p_tenMin_volumePrMin, p_tenMin_tradesPrMin, p_tenMin_buysPrMin, p_tenMin_sellsPrMin, p_fifteenMin_avgPrice, p_fifteenMin_highPrice, p_fifteenMin_lowPrice, p_fifteenMin_startPrice, p_fifteenMin_endPrice, p_fifteenMin_changeReal, p_fifteenMin_changePercent, p_fifteenMin_travelReal, p_fifteenMin_travelPercent, p_fifteenMin_volumePrMin, p_fifteenMin_tradesPrMin, p_fifteenMin_buysPrMin, p_fifteenMin_sellsPrMin, p_thirtyMin_avgPrice, p_thirtyMin_highPrice, p_thirtyMin_lowPrice, p_thirtyMin_startPrice, p_thirtyMin_endPrice, p_thirtyMin_changeReal, p_thirtyMin_changePercent, p_thirtyMin_travelReal, p_thirtyMin_travelPercent, p_thirtyMin_volumePrMin, p_thirtyMin_tradesPrMin, p_thirtyMin_buysPrMin, p_thirtyMin_sellsPrMin, p_sixtyMin_avgPrice, p_sixtyMin_highPrice, p_sixtyMin_lowPrice, p_sixtyMin_startPrice, p_sixtyMin_endPrice, p_sixtyMin_changeReal, p_sixtyMin_changePercent, p_sixtyMin_travelReal, p_sixtyMin_travelPercent, p_sixtyMin_volumePrMin, p_sixtyMin_tradesPrMin, p_sixtyMin_buysPrMin, p_sixtyMin_sellsPrMin, p_oneTwentyMin_avgPrice, p_oneTwentyMin_highPrice, p_oneTwentyMin_lowPrice, p_oneTwentyMin_startPrice, p_oneTwentyMin_endPrice, p_oneTwentyMin_changeReal, p_oneTwentyMin_changePercent, p_oneTwentyMin_travelReal, p_oneTwentyMin_travelPercent, p_oneTwentyMin_volumePrMin, p_oneTwentyMin_tradesPrMin, p_oneTwentyMin_buysPrMin, p_oneTwentyMin_sellsPrMin, coinbase_price, binance_price, huobi_price, bittrex_price, bch_price, eth_price, f_fiveSec_endPrice, f_thirtySec_endPrice, f_fifteenMin_endPrice, f_oneTwentyMin_endPrice"

selectForPrep = """
SELECT %s
FROM prepedTrades
LIMIT 200000
"""


# pd.set_option('display.max_columns', None)  # or 1000
# pd.set_option('display.max_rows', None)  # or 1000
# pd.set_option('display.max_colwidth', None)  # or 199

conn = connect()
df = pd.read_sql(selectForPrep % selectCols,conn)
conn.close()

print(df.describe())
columns = [ "p_fiveSec_avgPrice", "p_fiveSec_highPrice", "p_fiveSec_lowPrice", "p_fiveSec_startPrice", "p_fiveSec_endPrice", "p_tenSec_avgPrice", "p_tenSec_highPrice", "p_tenSec_lowPrice", "p_tenSec_startPrice", "p_tenSec_endPrice", "p_thirtySec_avgPrice", "p_thirtySec_highPrice", "p_thirtySec_lowPrice", "p_thirtySec_startPrice", "p_thirtySec_endPrice", "p_oneMin_avgPrice", "p_oneMin_highPrice", "p_oneMin_lowPrice", "p_oneMin_startPrice", "p_oneMin_endPrice", "p_threeMin_avgPrice", "p_threeMin_highPrice", "p_threeMin_lowPrice", "p_threeMin_startPrice", "p_threeMin_endPrice", "p_fiveMin_avgPrice", "p_fiveMin_highPrice", "p_fiveMin_lowPrice", "p_fiveMin_startPrice", "p_fiveMin_endPrice", "p_tenMin_avgPrice", "p_tenMin_highPrice", "p_tenMin_lowPrice", "p_tenMin_startPrice", "p_tenMin_endPrice", "p_fifteenMin_avgPrice", "p_fifteenMin_highPrice", "p_fifteenMin_lowPrice", "p_fifteenMin_startPrice", "p_fifteenMin_endPrice", "p_thirtyMin_avgPrice", "p_thirtyMin_highPrice", "p_thirtyMin_lowPrice", "p_thirtyMin_startPrice", "p_thirtyMin_endPrice", "p_sixtyMin_avgPrice", "p_sixtyMin_highPrice", "p_sixtyMin_lowPrice", "p_sixtyMin_startPrice", "p_sixtyMin_endPrice", "p_oneTwentyMin_avgPrice", "p_oneTwentyMin_highPrice", "p_oneTwentyMin_lowPrice", "p_oneTwentyMin_startPrice", "p_oneTwentyMin_endPrice", "coinbase_price", "binance_price", "huobi_price", "bittrex_price", "bch_price", "eth_price" ]
df = remove_outliers(df, columns, 3)
print(df.describe())
conn = connect()
df.to_sql("stdDevTrades", conn)
conn.close()
# df[np.abs(df.f_fiveSec_endPrice - df.f_fiveSec_endPrice.mean()) <= (3*df.f_fiveSec_endPrice.std())]

# df['f_fiveSec_endPrice'].describe()