# %%
import psycopg as pg
import pandas as pd
from progressbar import progressbar

# %%
with pg.connect("dbname = whales user = postgres password = NURAFIN") as connection:
    cursor = connection.cursor()
    walletswithbalancetimeseries = cursor.execute("SELECT DISTINCT(address) FROM public.walletbalancetimeseries").fetchall()


# %%
    walletswithbalancetimeseries = list(map(lambda tuple: tuple[0], walletswithbalancetimeseries))

# %%
with pg.connect("dbname = whales user = postgres password = NURAFIN") as connection:
    cursor = connection.cursor()
    for timeframems, numberofcandlesinperiod in [(24*60*60*1000, 30), (4*60*60*1000, 42)]:
        for walletaddress in progressbar(walletswithbalancetimeseries):
            df = pd.DataFrame(columns= ['time', 'balance_btc', 'relativebalance', 'btc_price', 'relativeprice'])
            wallettimeseries = cursor.execute("SELECT time, balance_btc FROM public.walletbalancetimeseries WHERE (address = %s AND time >= 1514764800000 AND MOD(time, %s) = 0) ORDER BY time ASC", (walletaddress, timeframems)).fetchall()
            firstwalletbalance = wallettimeseries[0]
            referencebalance = firstwalletbalance[1]
            firstcandletime = firstwalletbalance[0]
            referenceprice = cursor.execute("SELECT close FROM public.klines WHERE time = %s", (firstcandletime,)).fetchall()[0][0]
            for timestamp, balance in progressbar(wallettimeseries): 
                correspondingprice = cursor.execute("SELECT close FROM public.klines WHERE time = %s", (timestamp,)).fetchall()[0][0]
                if timestamp % (timeframems * numberofcandlesinperiod) == 0:
                    referencebalance = balance
                    referenceprice = correspondingprice
                if referencebalance != 0:
                    relativebalance = balance
                else:
                    relativebalance = 1
                if referenceprice != 0:
                    relativeprice = correspondingprice/referenceprice
                else:
                    relativeprice = 1
                newrow = pd.Series({'time': timestamp, 'balance_btc': balance, 'relativebalance': relativebalance, 'btc_price': correspondingprice, 'relativeprice': relativeprice})
                df = pd.concat([df, newrow.to_frame().T], ignore_index= True)



