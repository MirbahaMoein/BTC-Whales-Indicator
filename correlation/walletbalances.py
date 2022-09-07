import psycopg as pg
from tqdm import tqdm


def init_db():
    connection = pg.connect(
        "dbname = NURAFIN user = postgres password = NURAFIN")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS historicalwalletbalance (address VARCHAR(150), starttime bigint, endtime bigint, balance_btc real, PRIMARY KEY(starttime, address))")
    connection.commit()
    return connection, cursor


def fetch_wallets(cursor):
    wallets = cursor.execute(
        "SELECT DISTINCT address FROM public.transactions").fetchall()
    return wallets


def generate_balance_timeseries(wallets, connection, cursor):
    firstpricecandlesaved = cursor.execute("SELECT MAX(time) FROM public.klines").fetchall()[0][0]
    lastpricecandlesaved = cursor.execute("SELECT MAX(time) FROM public.klines").fetchall()[0][0]
    for wallet in tqdm(wallets, desc='Wallet', position=0):
        address = wallet[0]
        txs = cursor.execute(
            "SELECT time, balance_btc FROM public.transactions WHERE address = %s", (address,)).fetchall()
        firsttx = cursor.execute(
            "SELECT time FROM public.transactions WHERE address = %s ORDER BY time ASC LIMIT 1", (address,)).fetchall()
        firsttxtime = firsttx[0][0]
        lasttx = cursor.execute(
            "SELECT time, balance_btc FROM public.transactions WHERE address = %s ORDER BY time DESC LIMIT 1", (address,)).fetchall()
        lasttxtime = lasttx[0][0]
        lastbalance = lasttx[0][1]
        if firsttxtime > firstpricecandlesaved:
            cursor.execute("INSERT INTO public.historicalwalletbalance VALUES (%s, %s, %s, %s)", (
                    address, firstpricecandlesaved, firsttxtime - (firsttxtime % 3600000), 0))
        if lasttxtime < lastpricecandlesaved:
            cursor.execute("INSERT INTO public.historicalwalletbalance VALUES (%s, %s, %s, %s)", (
                    address, lasttxtime, lastpricecandlesaved, lastbalance))
        for itrtr in tqdm(range(len(txs)-1), desc='Transaction', position=1, leave=False):
            tx1 = txs[itrtr]
            tx1time = tx1[0]
            firstcandletime = tx1time - (tx1time % 3600000)
            tx1balance = tx1[1]
            tx2 = txs[itrtr + 1]
            tx2time = tx2[0]
            lastcandletime = tx2time - (tx2time % 3600000)
            try:
                cursor.execute("INSERT INTO public.historicalwalletbalance VALUES (%s, %s, %s, %s)", (
                    address, firstcandletime, lastcandletime, tx1balance))
            except:
                cursor.execute("ROLLBACK")
        connection.commit()


def main():
    connection, cursor = init_db()
    wallets = fetch_wallets(cursor)
    generate_balance_timeseries(wallets, connection, cursor)
    connection.close()


main()
