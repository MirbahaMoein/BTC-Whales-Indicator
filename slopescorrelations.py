import psycopg as postgres
from datetime import datetime
import numpy as np
from sklearn.linear_model import LinearRegression
import pandas as pd
from tqdm import tqdm


def connect_db():
    global connection, cursor
    connection = postgres.connect(
        "dbname = NURAFIN user = postgres password = NURAFIN")
    cursor = connection.cursor()
    cursor.execute(
        "DROP TABLE IF EXISTS slopes")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS slopes (starttime bigint, endtime bigint, walletaddress varchar(255), priceslope double precision, walletslope double precision, PRIMARY KEY(starttime, walletaddress))")



def timeframes(timeframedays: int):
    timeframems = timeframedays * 24 * 60 * 60 * 1000
    firstcandletime = cursor.execute(
        "SELECT MIN(time) FROM public.klines").fetchall()[0][0]
    return int(timeframems), int(firstcandletime)


def getpricerows(timeframestart: int, timeframestop: int) -> list:
    rows = cursor.execute("SELECT time, close FROM public.klines WHERE (time >= %s AND time < %s) ORDER BY time ASC",
                          (timeframestart, timeframestop)).fetchall()
    return rows


def getbalancerows(walletaddress: str, timeframestart: int, timeframestop: int) -> list:
    rows = cursor.execute("SELECT time, balance_btc FROM public.transactions WHERE (address = %s AND time >= %s AND time < %s) ORDER BY time ASC",
                          (walletaddress, timeframestart, timeframestop)).fetchall()
    return rows


def regressionslope(rows: list) -> float:
    try:
        ys = []
        xs = []
        for row in rows:
            ys.append(row[1])
            xs.append(row[0])
        x = np.array(xs).reshape((-1, 1))
        y = np.array(ys)
        model = LinearRegression().fit(x, y)
        coef = float(model.coef_[0])
        return coef
    except:
        return 0


def get_wallet_addresses() -> list:
    wallets = cursor.execute("""SELECT address FROM public.wallets
        ORDER BY rank DESC""").fetchall()
    return wallets


def slopes(wallets):
    timeframems, firstcandletime = timeframes(7)
    loopstop = int(datetime.now().timestamp()*1000 - timeframems)

    for timestamp in tqdm(range(firstcandletime, loopstop, timeframems)):
        timeframestart = timestamp
        timeframestop = timestamp + timeframems

        prices = getpricerows(timeframestart, timeframestop)
        priceslope = regressionslope(prices)

        for wallet in wallets:
            balances = getbalancerows(wallet[0], timeframestart, timeframestop)
            balanceslope = regressionslope(balances)
            try:
                cursor.execute("INSERT INTO public.slopes VALUES (%s, %s, %s, %s, %s)",
                               (timeframestart, timeframestop, wallet[0], priceslope, balanceslope))
            except:
                cursor.execute("ROLLBACK")
                cursor.execute("UPDATE public.slopes SET priceslope = %s, walletslope = %s WHERE (starttime = %s AND walletaddress = %s)",
                               (priceslope, balanceslope, timeframestart, wallet[0]))

            connection.commit()


def correlations(wallets):
    for wallet in tqdm(wallets):
        walletaddress = wallet[0]
        table = cursor.execute(
            "SELECT * FROM public.slopes WHERE walletaddress = %s", (walletaddress,)).fetchall()
        df = pd.DataFrame(table, columns=[
                          'starttime', 'endtime', 'walletaddress', 'priceslope', 'walletslope'])
        correlation = df['priceslope'].corr(df['walletslope'])
        cursor.execute(
            "UPDATE public.wallets SET balance_price_correlation = %s WHERE address = %s", (correlation, walletaddress))
        connection.commit()


def main():
    connect_db()
    wallets = get_wallet_addresses()
    slopes(wallets)
    correlations(wallets)
    connection.close()


main()
