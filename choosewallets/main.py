import psycopg as pg
from scrapedata.pricecandles import updateklines
from scrapedata.walletsdata import walletstable, updatewallets, updatetxs
from correlation.walletbalances import fetchwalletsintransactions, updatehistoricalwalletbalances
from correlation.correlations import fetchwalletswithbalancedata, generate_dataframe, updatecorrelations
from datetime import datetime


def choosewallets(cursor):
    wallets = cursor.execute("SELECT * FROM public.wallets")


def create_db(credentials, dbname):
    """with pg.connect(credentials) as connection:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE {}".format(dbname))
        connection.commit()"""
    pass


def main():
    symbol = 'BTCUSDT'
    pricecandletimeframems = 60000
    correlationcalculationtimeframems = 86400000
    firstpricecandletime = datetime(2018, 1, 1).timestamp()*1000
    credentials = "user = postgres password = NURAFIN"
    dbname = 'whales'
    #create_db(credentials, dbname)
    connectioninfo = "dbname = {} ".format(dbname) + credentials
    with pg.connect(connectioninfo) as connection:
        cursor = connection.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS klines (time bigint PRIMARY KEY, open real, high real, low real, close real, volume real)")
        cursor.execute("CREATE TABLE IF NOT EXISTS wallets (url varchar(200), rank smallint, bestrank smallint, address varchar(100) PRIMARY KEY, walletname varchar(50), multisig varchar(50), balance_BTC double precision, topbalance_BTC double precision, firstin bigint, lastin bigint, firstout bigint, lastout bigint, ins integer, outs integer, updated boolean, partial boolean, balance_price_correlation real)")
        cursor.execute("CREATE TABLE IF NOT EXISTS transactions (address varchar(100) REFERENCES wallets (address), blocknumber integer, time bigint, amount_BTC double precision, balance_BTC double precision, balance_USD real, accprofit_USD real, PRIMARY KEY(address, time, balance_BTC))")
        cursor.execute("DROP TABLE IF EXISTS historicalwalletbalance")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS historicalwalletbalance (address VARCHAR(150), starttime bigint, endtime bigint, balance_btc double precision, PRIMARY KEY(starttime, address))")
        cursor.execute(
            "UPDATE public.wallets SET balance_price_correlation = 0")
        connection.commit()
        updateklines(symbol, pricecandletimeframems,
                     firstpricecandletime, connection, cursor)
        updatewallets(connection, cursor)
        savedwallets = walletstable(cursor)
        updatetxs(savedwallets[::100], connection, cursor)
        walletswithsavedtxs = fetchwalletsintransactions(cursor)
        updatehistoricalwalletbalances(walletswithsavedtxs, connection, cursor)
        walletswithbalancedata = fetchwalletswithbalancedata(cursor)
        updatecorrelations(walletswithbalancedata, connection,
                           cursor, correlationcalculationtimeframems)


if __name__ == '__main__':
    main()