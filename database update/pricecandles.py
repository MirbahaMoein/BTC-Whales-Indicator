import binance.spot
import pandas as pd
from datetime import *
import psycopg
from tqdm import tqdm


def connectdb() -> tuple[psycopg.Connection, psycopg.Cursor]:
    connection = psycopg.connect(
        "dbname = NURAFIN user = postgres password = NURAFIN")
    cursor = connection.cursor()
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


def updateklines():
    connection, cursor = connectdb()
    firsttimestamp, lasttimestamp, nowtimestamp = generatetimestamps(cursor)

    client = binance.spot.Spot()

    timestamp = nowtimestamp

    pbar = tqdm(total = (nowtimestamp - datetime(2018, 1, 1).timestamp()*1000))

    while timestamp > lasttimestamp - 1000*60*1000:
  
        try:
            table = client.klines(
                "BTCUSDT", "1m", startTime=timestamp - 1000*60*1000, endTime=timestamp, limit=1000)
            data = pd.DataFrame(table)
            data.columns = ["open_timestamp", "open", "high", "low", "close", "volume", "close_timestamp",
                            "qvolume", "trades_number", "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"]

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
                    pass
                connection.commit()
        except:
            break
        timestamp -= 1000*60*1000
        pbar.update(1000*60*1000)

    timestamp = firsttimestamp + 1000*60*1000

    pbar = tqdm(total = (nowtimestamp - datetime(2018, 1, 1).timestamp()*1000))

    while timestamp > datetime(2018, 1, 1).timestamp()*1000:
        try:
            table = client.klines(
                "BTCUSDT", "1m", startTime=timestamp - 1000*60*1000, endTime=timestamp, limit=1000)
            data = pd.DataFrame(table)
            data.columns = ["open_timestamp", "open", "high", "low", "close", "volume", "close_timestamp",
                            "qvolume", "trades_number", "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"]

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
                    pass
                connection.commit()
        except:
            break
        timestamp -= 1000*60*1000

        pbar.update(1000*60*1000)

    connection.close()


updateklines()
