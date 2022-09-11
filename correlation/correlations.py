import psycopg as pg
from tqdm import tqdm
import pandas as pd


def fetch_wallets(cursor):
    return cursor.execute("SELECT DISTINCT(address) FROM public.historicalwalletbalance").fetchall()


def generate_dataframe(address, klines, cursor):
    walletdf = pd.DataFrame(columns=['time', 'btc_price', 'balance'])
    for kline in tqdm(klines, desc='Candles', position=1, leave=False):
        timestamp = kline[0]
        btcprice = kline[1]
        timespans = cursor.execute(
            "SELECT balance_btc FROM public.historicalwalletbalance WHERE (starttime < %s AND endtime > %s AND address = %s)", (timestamp, timestamp, address)).fetchall()
        if len(timespans) > 1:
            print("kline in more than one timespan",
                  address, '\n', kline, '\n', timespans)
            cursor.execute(
                "UPDATE public.wallets SET balance_price_correlation = 'NaN' WHERE address = %s", (address,))
            return False
        elif len(timespans) == 0:
            print("kline in no timespan", address, '\n', kline)
            cursor.execute(
                "UPDATE public.wallets SET balance_price_correlation = 'NaN' WHERE address = %s", (address,))
            return False
        else:
            walletdf.loc[len(walletdf)] = {
                'time': timestamp, 'btc_price': btcprice, 'balance': timespans[0][0]}
    return walletdf


def generate_correlations(wallets, connection, cursor, timeframe):
    klines = cursor.execute(
        "SELECT time, close FROM public.klines WHERE MOD(time, %s) = 0", (timeframe,)).fetchall()
    for wallet in tqdm(wallets, desc='Wallets', position=0):
        address = wallet[0]
        walletdf = generate_dataframe(address, klines, cursor)
        if type(walletdf) != bool:
            walletdf = prepare_dataframe(walletdf)
            cursor.execute(
                "UPDATE public.wallets SET balance_price_correlation = %s WHERE address = %s", (calculate_correlation(walletdf), address))
            connection.commit()


def prepare_dataframe(walletdf):
    walletdf = walletdf.sort_values(by='time')
    walletdf['balancetrend'] = walletdf['balance'] - \
        walletdf['balance'].rolling(4).mean()
    walletdf['pricetrend'] = walletdf['btc_price'] - \
        walletdf['btc_price'].rolling(4).mean()
    return walletdf


def calculate_correlation(walletdf):
    correlation = walletdf['balancetrend'].corr(walletdf['pricetrend'])
    return correlation


def main():
    with pg.connect("dbname = NURAFIN user = postgres password = NURAFIN") as connection:
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE public.wallets SET balance_price_correlation = 0")
        connection.commit()
        wallets = fetch_wallets(cursor)
        generate_correlations(wallets, connection, cursor, 604800000)
        connection.close()


