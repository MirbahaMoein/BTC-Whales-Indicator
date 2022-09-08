import binance.spot
import pandas as pd
from datetime import *
import time
import psycopg
from tqdm import tqdm


def connectdb() -> tuple[psycopg.Connection, psycopg.Cursor]:
    connection = psycopg.connect(
        "dbname = NURAFIN user = postgres password = NURAFIN")
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS klines (time bigint PRIMARY KEY, open real, high real, low real, close real, volume real)")
    connection.commit()
    return connection, cursor


def generatetimestamps(cursor) -> tuple:
    try:
        lastopentimestamp = int(cursor.execute(
            "SELECT MAX(time) FROM public.klines").fetchall()[0][0])
    except:
        lastopentimestamp = 0
    try:
        firstopentimestamp = int(cursor.execute(
            "SELECT MIN(time) FROM public.klines").fetchall()[0][0])
    except:
        firstopentimestamp = int(datetime.now().timestamp() * 1000)

    nowtimestamp = int(datetime.now().timestamp()*1000)

    return firstopentimestamp, lastopentimestamp, nowtimestamp


def init_client():
    return binance.spot.Spot()


def get_table(client, symbol, endtimestamp, timeframe):
    try:
        table = client.klines(symbol, "1m", startTime=endtimestamp -
                              timeframe, endTime=endtimestamp, limit=1000)
        if len(table) > 0:
            data = pd.DataFrame(table)
            data.columns = ["open_timestamp", "open", "high", "low", "close", "volume", "close_timestamp",
                        "qvolume", "trades_number", "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"]
            return data
        else:
            return []
    except:
        print('error getting table!')
        time.sleep(1)
        return get_table(client, symbol, endtimestamp, timeframe)

 
def updateklines(symbol, timeframe):
    startingtime = datetime(2018, 1, 1).timestamp()*1000
    connection, cursor = connectdb()
    client = init_client()
    firsttimestamp, lasttimestamp, nowtimestamp = generatetimestamps(cursor)
    timestamp = nowtimestamp
    pbar = tqdm(total= int((nowtimestamp - startingtime + firsttimestamp - lasttimestamp + 2 * timeframe) / (timeframe * 1000)))
    while timestamp > lasttimestamp - timeframe:
        data = get_table(client, symbol, timestamp, timeframe*1000)
        for i in range(len(data)):
            dtime = int(data["open_timestamp"][i])
            open = data["open"][i]
            high = data["high"][i]
            low = data["low"][i]
            close = data["close"][i]
            volume = data["volume"][i]
            try:
                cursor.execute("INSERT INTO klines VALUES (%s,%s,%s,%s,%s,%s)",
                               (dtime, open, high, low, close, volume))
            except:
                cursor.execute("ROLLBACK")
        connection.commit()
        timestamp -= timeframe * 1000
        pbar.update(1)

    timestamp = firsttimestamp + timeframe
    while timestamp > startingtime:
        data = get_table(client, symbol, timestamp, timeframe*1000)
        for i in range(len(data)):
            dtime = int(data["open_timestamp"][i])
            open = data["open"][i]
            high = data["high"][i]
            low = data["low"][i]
            close = data["close"][i]
            volume = data["volume"][i]
            try:
                cursor.execute("INSERT INTO klines VALUES (%s,%s,%s,%s,%s,%s)",
                               (dtime, open, high, low, close, volume))
            except:
                cursor.execute("ROLLBACK")
        connection.commit()
        timestamp -= timeframe * 1000
        pbar.update(1)

    connection.close()


updateklines(symbol="BTCUSDT", timeframe=60000)
