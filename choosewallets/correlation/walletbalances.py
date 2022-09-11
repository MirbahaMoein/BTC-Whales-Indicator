import psycopg as pg
from tqdm import tqdm


def fetchwalletsintransactions(cursor):
    wallets = cursor.execute(
        "SELECT DISTINCT address FROM public.transactions").fetchall()
    return wallets


def updatehistoricalwalletbalances(wallets, connection, cursor):
    firstsavedtxtime = cursor.execute(
        "SELECT MIN(time) FROM public.transactions").fetchall()[0][0]
    lastklinesavedtime = cursor.execute(
        "SELECT MAX(time) FROM public.klines").fetchall()[0][0]
    for wallet in tqdm(wallets, desc='Wallet', position=0):
        address = wallet[0]
        txs = cursor.execute(
            "SELECT time, balance_btc FROM public.transactions WHERE address = %s ORDER BY time ASC", (address,)).fetchall()
        firsttxtime = txs[0][0]
        cursor.execute("INSERT INTO public.historicalwalletbalance VALUES (%s, %s, %s, 0)",
                       (address, firstsavedtxtime, firsttxtime-1))
        lasttxtime = txs[-1][0]
        lasttxbalance = txs[-1][1]
        cursor.execute("INSERT INTO public.historicalwalletbalance VALUES (%s, %s, %s, %s)",
                       (address, lasttxtime, lastklinesavedtime, lasttxbalance))
        for itrtr in tqdm(range(len(txs)-1), desc='Transaction', position=1, leave=False):
            tx1 = txs[itrtr]
            tx1time = tx1[0]
            tx1balance = tx1[1]
            tx2 = txs[itrtr + 1]
            tx2time = tx2[0]
            try:
                cursor.execute("INSERT INTO public.historicalwalletbalance VALUES (%s, %s, %s, %s)", (
                    address, tx1time, tx2time-1, tx1balance))
            except:
                cursor.execute("ROLLBACK")
        connection.commit()
