import psycopg as pg
from tqdm import tqdm


connection = pg.connect("dbname = NURAFIN user = postgres password = NURAFIN")
cursor = connection.cursor()


cursor.execute("DROP TABLE IF EXISTS public.historicalbalance")
cursor.execute("CREATE TABLE IF NOT EXISTS historicalbalance (time bigint PRIMARY KEY, totalbalance_btc integer, btcprice integer, totalbalance100_usd bigint, totalbalance500_usd bigint, totalbalance1000_usd bigint)")


klines = cursor.execute(
    "SELECT * FROM public.klines ORDER BY time ASC").fetchall()
selectedwallets = cursor.execute(
    "SELECT DISTINCT address, balance_price_correlation FROM public.wallets WHERE balance_price_correlation != 'NaN' AND balance_price_correlation > 0 ORDER BY balance_price_correlation DESC").fetchall()


for itrtr in tqdm(range(0, len(klines), 60), desc = 'time', position= 0):
    kline = klines[itrtr]
    candletime = kline[0]
    candleopen = kline[1]
    candlehigh = kline[2]
    candlelow = kline[3]
    candleclose = kline[4]
    candlevolume = kline[5]
    totalbalance = 0
    for wallet in tqdm(enumerate(selectedwallets), desc= 'wallet', position = 1, leave = False):
        walletrank = wallet[0]
        walletaddress = wallet[1][0]
        try:
            lasttx = cursor.execute(
                "SELECT * FROM public.transactions WHERE time < %s AND address = %s ORDER BY time DESC", (candletime, walletaddress)).fetchone()
            walletbalance = lasttx[4]
            totalbalance += walletbalance
        except:
            pass

    try:
        cursor.execute("INSERT INTO public.historicalbalance VALUES (%s, %s, %s, %s)",
                       (candletime, totalbalance, candleopen, totalbalance * candleopen))
    except:
        cursor.execute("ROLLBACK")
        cursor.execute("UPDATE public.historicalbalance SET totalbalance_btc = %s, btcprice = %s, totalbalance_usd = %s WHERE time = %s",
                       (totalbalance, candleopen, totalbalance * candleopen, candletime))
    connection.commit()

connection.close()
