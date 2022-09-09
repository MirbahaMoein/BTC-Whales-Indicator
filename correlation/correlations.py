import psycopg as pg
from tqdm import tqdm
import pandas as pd
import sys

def connect_db():
    connection = pg.connect(
        "dbname = NURAFIN user = postgres password = NURAFIN")
    cursor = connection.cursor()
    return connection, cursor


def fetch_wallets(cursor):
    return cursor.execute("SELECT DISTINCT(address) FROM public.historicalwalletbalance").fetchall()


def calculate_correlation(wallets, connection, cursor):
    for wallet in tqdm(wallets, desc= 'wallet', position= 0):
        walletdf = pd.DataFrame(columns = ['time', 'btc_price', 'balance'])
        address = wallet[0]
        equalbalancetimespans = cursor.execute("SELECT starttime, endtime, balance_btc FROM public.historicalwalletbalance WHERE (address = %s)",(address,)).fetchall()
        for timespan in tqdm(equalbalancetimespans, desc= 'timespan', position = 1, leave= False):
            starttime = timespan[0]
            endtime = timespan[1]
            balance = timespan[2]
            klines = cursor.execute("SELECT time, close FROM public.klines WHERE (time >= %s AND time <= %s AND MOD(time, 3600000) = 0)", (starttime, endtime)).fetchall()
            for kline in tqdm(klines, desc= 'klines between', position = 2, leave= False):
                timestamp = kline[0]
                price = kline[1]
                walletdf.loc[len(walletdf)] = {'time': timestamp, 'btc_price': price, 'balance': balance}
        walletdf = walletdf.sort_values(by= 'time')
        correlation = walletdf['btc_price'].corr(walletdf['balance'])
        cursor.execute("UPDATE public.wallets SET balance_price_correlation = %s WHERE address = %s", (correlation, address))
        connection.commit()


def main():
    connection, cursor = connect_db()
    wallets = fetch_wallets(cursor)
    calculate_correlation(wallets, connection, cursor)
    connection.close()


main()
