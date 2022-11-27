import psycopg as pg
from tqdm import tqdm
import pandas as pd
import numpy as np


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
        numberoftxs = cursor.execute("SELECT COUNT(*) FROM public.historicalwalletbalance WHERE (starttime <= %s AND starttime >= %s AND address = %s)", (end, start, address)).fetchall()[0][0]
        if numberoftxs > 0:
            walletdf = generate_dataframe(address, klines, cursor)
            if type(walletdf) != bool:
                walletdf = prepare_dataframe(walletdf)
                correlation = calculate_correlation(walletdf, lag)
                cursor.execute(
                    "UPDATE public.wallets SET balance_price_correlation = %s WHERE address = %s", (correlation, address))
                connection.commit()
        else:
            cursor.execute(
                "UPDATE public.wallets SET balance_price_correlation = -1 WHERE address = %s", (address,))
            connection.commit()


def update_correlations(wallets, connection, cursor, start, end, timeframems, speriod, lperiod, lag):
    cursor.execute("UPDATE public.wallets SET balance_price_correlation = 0")
    cursor.execute("CREATE TABLE IF NOT EXISTS correlations (address varchar(100), speriod smallint, lperiod smallint, lag smallint, timeframems bigint, periodstart bigint, periodend bigint, correlation real, PRIMARY KEY(address, speriod, lperiod, lag, timeframems, periodstart, periodend))")
    connection.commit()
    klines = cursor.execute("SELECT time, close FROM public.klines WHERE (MOD(time, %s) = 0 AND time >= %s AND time <= %s) ORDER BY time ASC", (timeframems, start, end)).fetchall()
    klinesdf = pd.DataFrame(klines, columns= ['time', 'close'])
    for wallet in wallets:
        address = wallet[0]
        balancetimeseries = cursor.execute("SELECT time, balance_btc FROM public.walletbalancetimeseries WHERE (address = %s AND time > %s AND time <= %s AND MOD(time, %s) = 0) ORDER BY time ASC", (address, start, end, timeframems)).fetchall()
        balancedf = pd.DataFrame(balancetimeseries, columns= ['time', 'balance'])
        calcdf = klinesdf.join(balancedf.set_index("time"), on= 'time')
        calcdf = calcdf.sort_values(by= 'time')
        calcdf['balancetrend'] = calcdf['balance'].ewm(span= speriod).mean() - calcdf['balance'].ewm(span= lperiod).mean()
        calcdf['pricetrend'] = calcdf['close'].ewm(span= speriod).mean() - calcdf['close'].ewm(span= lperiod).mean()
        calcdf['pricetrend'] = calcdf.pricetrend.shift(-lag)
        correlation = calcdf['balancetrend'].corr(calcdf['pricetrend'])
        if correlation == np.nan:
            continue
        try:
            cursor.execute("INSERT INTO public.correlations VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (address, speriod, lperiod, lag, timeframems, start, end, correlation))
        except:
            connection.rollback()
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


def generate_wallet_timeseries(connection, cursor, wallets, timeframe, periodstart):
    cursor.execute("CREATE TABLE IF NOT EXISTS walletbalancetimeseries (address varchar(100), time bigint, balance_btc double precision, PRIMARY KEY(address, time))")
    for wallettuple in tqdm(wallets, desc= 'wallet', position= 0, leave= True):
        walletaddress = wallettuple[0]
        periods = cursor.execute("SELECT * FROM public.historicalwalletbalance WHERE address = %s ORDER BY starttime ASC", (walletaddress,)).fetchall()
        for periodtuple in tqdm(periods, desc= 'period', position= 1, leave= False):
            starttime = periodtuple[1]
            endtime = periodtuple[2]
            balance = periodtuple[3]
            if starttime % timeframe == 0:
                firstcandletime = starttime
            else:
                firstcandletime = starttime - (starttime % timeframe) + timeframe

            lastcandletime = endtime - (endtime % timeframe)

            if len(cursor.execute("SELECT * FROM public.walletbalancetimeseries WHERE (address = %s AND time = %s)", (walletaddress, lastcandletime)).fetchall()) == 1:
                continue

            candletimes = range(firstcandletime, lastcandletime +1, timeframe)

            for candletime in tqdm(candletimes, desc= 'candles', position = 2, leave= False):
                if candletime > periodstart:
                    try:
                        cursor.execute("INSERT INTO public.walletbalancetimeseries VALUES (%s, %s, %s)", (walletaddress, candletime, balance))
                        connection.commit()
                    except: 
                        connection.rollback()