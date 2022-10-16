from main import read_db_credentials
import psycopg as pg
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt

def generate_totalbalance_chart(timeframe: int):
    credentials, dbname = read_db_credentials()
    connectioninfo = "dbname = {} ".format(dbname) + credentials
    with pg.connect(connectioninfo) as connection:
        cursor = connection.cursor()
        klines = cursor.execute(
            "SELECT time, close FROM public.klines WHERE MOD(time, %s) = 0 ORDER BY time ASC", (timeframe,)).fetchall()
        df = pd.DataFrame(columns=['time', 'total_balance', 'btc_price'])
        for kline in tqdm(klines):
            timestamp = kline[0]
            btcprice = kline[1]
            totalbalance = cursor.execute(
                "SELECT SUM(balance_btc) FROM public.historicalwalletbalance WHERE (starttime <= %s AND endtime >= %s AND address IN (SELECT address FROM public.wallets WHERE (balance_price_correlation > 0 AND balance_price_correlation != 'NaN')))", (timestamp, timestamp)).fetchall()[0][0]
            new_row = pd.Series({'time': timestamp, 'total_balance': totalbalance, 'btc_price': btcprice})
            df = pd.concat([df, new_row.to_frame().T], ignore_index= True)
    df.plot(x= 'time', y= ['btc_price', 'total_balance'] , use_index= False, logy= True, secondary_y= 'total_balance')
    plt.show()

generate_totalbalance_chart(24*60*60*1000)