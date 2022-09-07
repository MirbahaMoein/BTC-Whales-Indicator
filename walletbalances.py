import psycopg as pg
from tqdm import tqdm

def init_db():
    connection = pg.connect("dbname = NURAFIN user = postgres password = NURAFIN")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS historicalwalletbalance (address VARCHAR(150), time bigint, balance_btc real, PRIMARY KEY(time, address))")
    connection.commit()
    return connection, cursor

def fetch_wallets(cursor):
    wallets = cursor.execute(
        "SELECT DISTINCT address FROM public.transactions").fetchall()
    return wallets

def generate_balance_timeseries(wallets, connection, cursor):

    for wallet in tqdm(wallets, desc = 'Wallet', position = 0):
        
        address = wallet[0]
        txs = cursor.execute("SELECT time, balance_btc FROM public.transactions WHERE address = %s", (address,)).fetchall()
        
        firsttxtime = txs[0][0]
        candlesbefore = cursor.execute("SELECT time FROM public.klines WHERE (time < %s AND time %% 3600000 = 0) ORDER BY time ASC", (firsttxtime,)).fetchall()
        for itrtr in tqdm(range(len(candlesbefore)), desc = 'Candles before', position = 1, leave = False):
            candletime = candlesbefore[itrtr][0]
            cursor.execute("INSERT INTO public.historicalwalletbalance VALUES (%s, %s, 0)", (address, candletime))

        for itrtr in tqdm(range(len(txs)-1), desc= 'Candles between', position = 1, leave = False):
            tx1 = txs[itrtr]
            tx1time = tx1[0]
            tx1balance = tx1[1]
            
            tx2 = txs[itrtr + 1]
            tx2time = tx2[0]
            
            candles = cursor.execute("SELECT time FROM public.klines WHERE (time < %s AND time >= %s AND time %% 3600000 = 0) ORDER BY time ASC", (tx2time, tx1time)).fetchall()
            for citrtr in range(len(candles)):
                candletime = candles[citrtr][0]
                cursor.execute("INSERT INTO public.historicalwalletbalance VALUES (%s, %s, %s)", (address, candletime, tx1balance))
        
        lasttx = txs[-1]
        lasttxtime = lasttx[0]
        lasttxbalance = lasttx[1]
        candlesafter = cursor.execute("SELECT time FROM public.klines WHERE (time >= %s AND time %% 3600000 = 0) ORDER BY time ASC", (lasttxtime,)).fetchall()
        for itrtr in tqdm(range(len(candlesafter)), desc = 'Candles after', position = 1, leave = False):
            candletime = candlesafter[itrtr][0]
            cursor.execute("INSERT INTO public.historicalwalletbalance VALUES (%s, %s, %s)", (address, candletime, lasttxbalance))

        connection.commit()


def main():
    connection, cursor = init_db()
    wallets = fetch_wallets(cursor)
    generate_balance_timeseries(wallets, connection, cursor)
    connection.close()

main()