import psycopg as pg
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
import pandas_ta as ta
from datetime import datetime


def generate_df(cursor, timeframe: int, fastema: int, slowema: int, corrthreshold: float, start: int, end: int):
    klines = cursor.execute("SELECT time, close FROM public.klines WHERE (MOD(time, %s) = 0 AND time >= %s AND time <= %s) ORDER BY time ASC", (timeframe, start, end)).fetchall()
    df = pd.DataFrame(columns=['time', 'total_balance', 'btc_price'])
    wallets = cursor.execute("SELECT address FROM public.correlations WHERE (speriod = 7 AND lperiod = 14 AND lag = 9 AND timeframems = 86400000 AND periodstart = 1577824200000 AND correlation > %s AND correlation != 'NaN')", (corrthreshold,)).fetchall()
    
    walletslist = []
    for wallet in wallets:
        walletslist.append(wallet[0])

    for kline in tqdm(klines):
        timestamp = kline[0]
        btcprice = kline[1]
        totalbalance = cursor.execute("SELECT SUM(balance_btc) FROM public.historicalwalletbalance WHERE (starttime <= %s AND endtime >= %s AND address = ANY(%s))", (timestamp, timestamp, walletslist)).fetchall()[0][0]
        new_row = pd.Series({'time': timestamp, 'total_balance': totalbalance, 'btc_price': btcprice})
        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
    
    df['time'] = pd.to_datetime(df['time'], unit='ms')

    return df


def generate_charts(df, fastema, slowema, bbspan):
    
    df['balance_trend'] = df['total_balance'].ewm(span=fastema).mean() - df['total_balance'].ewm(span=slowema).mean()
    std = 1
    bbands = ta.bbands(close= df['total_balance'], length= bbspan, std= std)
    df['bbl'] = bbands['BBL_{}_{}.0'.format(str(bbspan), str(std))]
    df['bbu'] = bbands['BBU_{}_{}.0'.format(str(bbspan), str(std))]
    df['bbm'] = bbands['BBM_{}_{}.0'.format(str(bbspan), str(std))]
    df['level0'] = 0
    
    plt.figure(1)

    plt.subplot(411)
    plt.plot(df['time'], df['btc_price'])
    plt.yscale('log')
    plt.title('Price')
    plt.grid(True)

    plt.subplot(413)
    plt.plot(df['time'], df['balance_trend'])
    plt.plot(df['time'], df['level0'], color='gray')
    plt.yscale('linear')
    plt.title('Relative Indicator Position in BB')
    plt.grid(True)

    plt.subplot(414)
    plt.plot(df['time'], (df['total_balance'] - df['bbl']) / (df['bbu'] - df['bbl']) - 0.5)
    plt.plot(df['time'], df['level0'], color='gray')
    plt.yscale('linear')
    plt.title('Total Balance')
    plt.grid(True)

    plt.subplot(412)
    plt.plot(df['time'], df['total_balance'])
    plt.yscale('log')
    plt.title('Total Balance')
    plt.grid(True)

    plt.show()


def save_feather(df):
    #df = df.drop(['btc_price'], axis=1)
    df = df.sort_values(by= 'time', ignore_index=True)
    df.to_feather("./dataset/indicator.ftr")


#with pg.connect("dbname = whales user = postgres password = NURAFIN") as connection:
#    cursor = connection.cursor()
#    df = pd.read_feather("./dataset/indicator.ftr")
#    #df = generate_df(cursor, 86400000, 7, 17, 0.25, datetime(2018,1,1).timestamp()*1000, datetime(2022,10,1).timestamp()*1000)
#    #save_feather(df)
#    fastema = 7
#    slowema = 17
#    bbspan = 20
#    generate_charts(df, fastema, slowema, bbspan)


df = pd.read_feather("./dataset/indicator.ftr")
fastema = 7
slowema = 17
bbspan = 50
generate_charts(df, fastema, slowema, bbspan)