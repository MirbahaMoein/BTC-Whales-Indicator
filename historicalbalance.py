import psycopg as pg
from tqdm import tqdm


connection = pg.connect("dbname = NURAFIN user = postgres password = NURAFIN")
cursor = connection.cursor()


cursor.execute("DROP TABLE IF EXISTS public.historicalbalance")
cursor.execute("CREATE TABLE IF NOT EXISTS historicalbalance (time bigint PRIMARY KEY, totalbalance_btc integer, totalbalance100_btc integer, totalbalance500_btc integer, totalbalance1000_btc integer, btcprice integer)")


klines = cursor.execute(
    "SELECT * FROM public.klines ORDER BY time ASC").fetchall()
selectedwallets = cursor.execute(
    "SELECT DISTINCT address, balance_price_correlation FROM public.wallets WHERE balance_price_correlation != 'NaN' ORDER BY balance_price_correlation DESC").fetchall()


for itrtr in tqdm(range(0, len(klines), 360), desc='time', position=0):
    candletime = klines[itrtr][0]
    candleopen = klines[itrtr][1]
    candlehigh = klines[itrtr][2]
    candlelow = klines[itrtr][3]
    candleclose = klines[itrtr][4]
    candlevolume = klines[itrtr][5]
    totalbalance100 = 0
    totalbalance500 = 0
    totalbalance1000 = 0
    totalbalance = 0
    for wallet in tqdm(enumerate(selectedwallets), desc='wallet', position=1, leave=False):
        walletrank = wallet[0]
        walletaddress = wallet[1][0]
        try:
            lasttx = cursor.execute(
                "SELECT * FROM public.transactions WHERE time < %s AND address = %s ORDER BY time DESC", (candletime, walletaddress)).fetchone()
            walletbalance = lasttx[4]
            if walletrank < 100:
                totalbalance100 += walletbalance
                totalbalance500 += walletbalance
                totalbalance1000 += walletbalance
                totalbalance += walletbalance
            elif walletrank < 500:
                totalbalance500 += walletbalance
                totalbalance1000 += walletbalance
                totalbalance += walletbalance
            elif walletrank < 1000:
                totalbalance1000 += walletbalance
                totalbalance += walletbalance
            else:
                totalbalance += walletbalance
        except:
            pass

    try:
        cursor.execute("INSERT INTO public.historicalbalance VALUES (%s, %s, %s, %s, %s, %s)",
                       (candletime, totalbalance, totalbalance100, totalbalance500, totalbalance1000, candleopen))
    except:
        cursor.execute("ROLLBACK")
        cursor.execute("UPDATE public.historicalbalance SET totalbalance_btc = %s, totalbalance100_btc = %s, totalbalance500_btc = %s, totalbalance1000_btc = %s, btcprice = %s WHERE time = %s",
                       (totalbalance, totalbalance100, totalbalance500, totalbalance1000, candleopen, candletime))
    connection.commit()

connection.close()
