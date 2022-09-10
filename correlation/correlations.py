import psycopg as pg
from tqdm import tqdm
import pandas as pd
import sys

def connect_db():
    connection = pg.connect(
        "dbname = NURAFIN user = postgres password = NURAFIN")
    cursor = connection.cursor()
    cursor.execute("UPDATE public.wallets SET balance_price_correlation = 0")
    connection.commit()
    return connection, cursor


def fetch_wallets(cursor):
    return cursor.execute("SELECT DISTINCT(address) FROM public.historicalwalletbalance").fetchall()


def calculate_correlation(wallets, connection, cursor, timeframe):
    klines = cursor.execute("SELECT time, close FROM public.klines WHERE MOD(time, %s) = 0", (timeframe,)).fetchall()
    for wallet in tqdm(wallets, desc= 'Wallets', position= 0):
        walletdf = pd.DataFrame(columns = ['time', 'btc_price', 'balance'])
        address = wallet[0]
        for kline in tqdm(klines, desc= 'Candles', position = 1, leave = False):
            timestamp = kline[0]
            btcprice = kline[1]
            timespans = cursor.execute("SELECT balance_btc FROM public.historicalwalletbalance WHERE (starttime < %s AND endtime > %s AND address = %s)",(timestamp, timestamp, address)).fetchall()
            if len(timespans) > 1:
                print("kline in more than one timespan")
                sys.exit()
            elif len(timespans) == 0:
                print("kline in no timespans")
                sys.exit()                
            else:
                walletdf.loc[len(walletdf)] = {'time': timestamp, 'btc_price': btcprice, 'balance': timespans[0][0]}
        walletdf = walletdf.sort_values(by= 'time')
        correlation = walletdf['btc_price'].corr(walletdf['balance'], method = 'spearman')
        cursor.execute("UPDATE public.wallets SET balance_price_correlation = %s WHERE address = %s", (correlation, address))
        connection.commit()


def main():
    connection, cursor = connect_db()
    wallets = fetch_wallets(cursor)
    calculate_correlation(wallets, connection, cursor, 604800000)
    connection.close()


main()
