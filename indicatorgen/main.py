import psycopg as pg
from showchart import generate_charts, save_feather, generate_df
from scrapedata.pricecandles import updateklines
from scrapedata.walletsdata import walletstable, updatewallets, updatetxs
from correlation.walletbalances import fetchwalletsintransactions, updatehistoricalwalletbalances
from correlation.correlations import fetchwalletswithbalancedata, generate_dataframe, updatecorrelations
from datetime import *
import json
from tqdm import tqdm


def read_db_credentials():
    with open("./indicatorgen/config.json") as config:
        data = json.load(config)
        dbname = data["dbname"]
        credentials = data["credentials"]
    return credentials, dbname


def main():
    runtime = int(datetime.now().timestamp()*1000)
    symbol = 'BTCUSDT'
    charttimeframe = 4 * 60 * 60 * 1000
    pricecandletimeframems = 60000
    corrmethod = 'pearson'
    lagbehind = 7
    correlationcalculationtimeframems = 4 * 60 * 60 * 1000
    corrcalcperiodstart = (datetime(2021, 5, 1)).timestamp() * 1000
    corrcalcperiodend = (datetime(2022, 1, 1)).timestamp() * 1000 
    firstpricecandletime = datetime(2018, 1, 1).timestamp() * 1000
    credentials, dbname = read_db_credentials()
    connectioninfo = "dbname = {} ".format(dbname) + credentials
    with pg.connect(connectioninfo) as connection:
        cursor = connection.cursor()
        #cursor.execute("CREATE TABLE IF NOT EXISTS klines (time bigint PRIMARY KEY, open real, high real, low real, close real, volume real)")
        #cursor.execute("CREATE TABLE IF NOT EXISTS wallets (url varchar(200), rank smallint, bestrank smallint, address varchar(100) PRIMARY KEY, walletname varchar(50), multisig varchar(50), balance_BTC double precision, topbalance_BTC double precision, firstin bigint, lastin bigint, firstout bigint, lastout bigint, ins integer, outs integer, updated boolean, partial boolean, balance_price_correlation real)")
        #cursor.execute("CREATE TABLE IF NOT EXISTS transactions (address varchar(100) REFERENCES wallets (address), blocknumber integer, time bigint, amount_BTC double precision, balance_BTC double precision, balance_USD real, accprofit_USD real, PRIMARY KEY(address, time, balance_BTC))")
        #connection.commit()
        #updateklines(symbol, pricecandletimeframems, firstpricecandletime, connection, cursor)
        #updatewallets(connection, cursor)
        #savedwallets = walletstable(cursor)
        #updatetxs(savedwallets, connection, cursor, runtime)
        #walletswithsavedtxs = fetchwalletsintransactions(cursor)
        #updatehistoricalwalletbalances(walletswithsavedtxs, connection, cursor)
        #walletswithbalancedata = fetchwalletswithbalancedata(cursor)
        #updatecorrelations(walletswithbalancedata, connection, cursor, corrcalcperiodstart, corrcalcperiodend,correlationcalculationtimeframems, lagbehind)
        maxcorrelation = cursor.execute("SELECT MAX(balance_price_correlation) FROM public.wallets WHERE balance_price_correlation != 'NaN'").fetchall()[0][0]
        for fastema in tqdm([2, 8, 24], position= 0):
            for slowema in tqdm([fastema * 2, fastema * 4], position = 1, leave= False):
                for corrthreshold in tqdm([0, maxcorrelation * 1/4, maxcorrelation * 1/2, maxcorrelation * 3/4], position = 2, leave= False):
                    df = generate_df(cursor, charttimeframe, fastema, slowema, corrthreshold, (datetime(2021, 12, 15)).timestamp() * 1000, (datetime(2022, 5, 1)).timestamp() * 1000)
                    #generate_charts(df)
                    save_feather(df, corrmethod, correlationcalculationtimeframems, corrthreshold, charttimeframe, lagbehind, fastema, slowema)


if __name__ == '__main__':
    main()