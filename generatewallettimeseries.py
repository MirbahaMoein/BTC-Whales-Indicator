import psycopg as pg
import pandas as pd
from datetime import datetime
from tqdm import tqdm

with pg.connect("dbname = whales user = postgres password = NURAFIN") as connection:
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS walletbalancetimeseries (address varchar(100), time bigint, balance_btc double precision, PRIMARY KEY(address, time))")
    wallets = cursor.execute("SELECT DISTINCT(address) FROM public.historicalwalletbalance").fetchall()
    timeframe = 4 * 60 * 60 * 1000
    for wallettuple in tqdm(wallets, desc= 'wallet', position= 0, leave= True):
        walletaddress = wallettuple[0]
        periods = cursor.execute("SELECT * FROM public.historicalwalletbalance WHERE address = %s ORDER BY starttime ASC", (walletaddress,)).fetchall()
        df = pd.DataFrame(columns = ['address', 'time', 'balance'])
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
                if candletime > datetime(2018,1,1).timestamp()*1000:
                    try:
                        cursor.execute("INSERT INTO public.walletbalancetimeseries VALUES (%s, %s, %s)", (walletaddress, candletime, balance))
                        connection.commit()
                    except: 
                        connection.rollback()
