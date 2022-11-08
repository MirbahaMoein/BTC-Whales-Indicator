import psycopg as pg
from tqdm import tqdm
import pandas as pd


def fetchwalletswithbalancedata(cursor):
    return cursor.execute("SELECT DISTINCT(address) FROM public.historicalwalletbalance").fetchall()


def generate_dataframe(address, klines, cursor):
    walletdf = pd.DataFrame(columns=['time', 'btc_price', 'balance'])
    for kline in tqdm(klines, desc='Candles', position=1, leave=False):
        timestamp = kline[0]
        btcprice = kline[1]
        timespan = cursor.execute(
            "SELECT balance_btc FROM public.historicalwalletbalance WHERE (starttime <= %s AND endtime >= %s AND address = %s)", (timestamp, timestamp, address)).fetchall()
        if len(timespan) > 1:
            print("kline in more than one timespan",
                  address, '\n', kline, '\n', timespan)
            cursor.execute(
                "UPDATE public.wallets SET balance_price_correlation = 'NaN' WHERE address = %s", (address,))
            return False
        elif len(timespan) == 0:
            print("kline in no timespan", address, '\n', kline)
            cursor.execute(
                "UPDATE public.wallets SET balance_price_correlation = 'NaN' WHERE address = %s", (address,))
            return False
        else:
            walletdf.loc[len(walletdf)] = {
                'time': timestamp, 'btc_price': btcprice, 'balance': timespan[0][0]}
    return walletdf


def updatecorrelations(wallets, connection, cursor, start, end, timeframe, lag):
    cursor.execute(
        "UPDATE public.wallets SET balance_price_correlation = 0")
    connection.commit()
    klines = cursor.execute(
        "SELECT time, close FROM public.klines WHERE (MOD(time, %s) = 0 AND time >= %s AND time <= %s)", (timeframe, start, end)).fetchall()
    for wallet in tqdm(wallets, desc='Wallets', position=0):
        address = wallet[0]
        walletdf = generate_dataframe(address, klines, cursor)
        if type(walletdf) != bool:
            walletdf = prepare_dataframe(walletdf)
            correlation = calculate_correlation(walletdf, lag)
            cursor.execute(
                "UPDATE public.wallets SET balance_price_correlation = %s WHERE address = %s", (correlation, address))
            connection.commit()


def prepare_dataframe(walletdf):
    walletdf = walletdf.sort_values(by='time')
    walletdf['balancetrend'] = walletdf['balance'].ewm(span=4).mean() - \
        walletdf['balance'].ewm(span=8).mean()
    walletdf['pricetrend'] = walletdf['btc_price'].ewm(span=4).mean() - \
        walletdf['btc_price'].ewm(span=8).mean()
    return walletdf


def calculate_correlation(walletdf, lag):
    walletdf = lag_behind(walletdf, lag)
    correlation = walletdf['balancetrend'].corr(walletdf['pricetrend'])
    return correlation


def lag_behind(df: pd.DataFrame, lag: int) -> pd.DataFrame:
    df = df.sort_values(by= 'time')
    df = df.reset_index(drop= True)
    newdf = pd.DataFrame(columns=['pricetrend', 'balancetrend'])
    for index in range(len(df) - lag):
        new_row = pd.Series({'pricetrend': df['pricetrend'][index + lag], 'balancetrend': df['balancetrend'][index]})
        newdf = pd.concat([newdf, new_row.to_frame().T], ignore_index=True)
    return newdf
