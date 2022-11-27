from indicatorgen.correlation.correlations import update_correlations
import psycopg as pg
from datetime import *
from tqdm import tqdm


charttimeframe = 4 * 60 * 60 * 1000
with pg.connect("dbname = whales user = postgres password = NURAFIN") as connection:
    cursor = connection.cursor()
    walletswithbalancetimeseries = cursor.execute("SELECT DISTINCT(address) FROM public.walletbalancetimeseries").fetchall()
    for correlationcalculationtimeframems in tqdm([4 * 60 * 60 * 1000, 24 * 60 * 60 * 1000], position= 0, leave= False, desc= 'timeframe'):
        for periodstart in tqdm([datetime(2018,1,1).timestamp()*1000, datetime(2018,7,1).timestamp()*1000, datetime(2019,1,1).timestamp()*1000], position = 1, leave = False, desc= 'period'):
            periodend = periodstart + int(timedelta(days = 3 * 365).total_seconds()*1000)
            for lperiod in tqdm(range(2, 31, 3), position= 2, leave= False, desc= 'lperiod'):
                for speriod in tqdm(range(1, int(lperiod / 2) + 1, 2), position = 3, leave = False, desc = 'speriod'):
                    for lag in tqdm(range(0, 31, 3), position= 4, leave= False, desc= 'lag'):
                        update_correlations(walletswithbalancetimeseries, connection, cursor, periodstart, periodend, correlationcalculationtimeframems, speriod, lperiod, lag) 