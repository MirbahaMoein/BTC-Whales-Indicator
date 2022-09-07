from statistics import correlation
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
    timestamps = cursor.execute(
        "SELECT time, close FROM public.klines WHERE MOD(time, 3600000) = 0 ORDER BY time ASC").fetchall()
    for wallet in tqdm(wallets):
        walletdf = pd.DataFrame(columns = ['time', 'btc_price', 'balance'])
        address = wallet[0]
        for row in timestamps:
            timestamp = row[0]
            btcprice = row[1]
            query = cursor.execute("SELECT balance_btc FROM public.historicalwalletbalance WHERE (address = %s AND starttime < %s AND endtime >= %s)", (
                address, timestamp, timestamp)).fetchall() # RETURNS NOTHING, WHY?????????????????????
            
            if len(query) == 1:
                balance = query[0][0]
            elif len(query) > 1:
                print(address, timestamp, query)
                sys.exit("something terrible happened: a candletime fits in more than one time range")
            else: 
                print(address, timestamp, query)
                sys.exit("something terrible happened: a candletime fits in no time range")

            walletdf = walletdf.append({'time': timestamp, 'btc_price': btcprice, 'balance': balance}, verify_integrity= True)
        walletdf = walletdf.sort_values(by= 'time', asc = True)
        correlation = walletdf['btc_price'].corr(walletdf['balance'])
        cursor.execute("UPDATE public.wallets SET balance_price_correlation = %s WHERE address = %s", (correlation, address))
        connection.commit()


def main():
    connection, cursor = connect_db()
    wallets = fetch_wallets(cursor)
    calculate_correlation(wallets, connection, cursor)
    connection.close()


main()
