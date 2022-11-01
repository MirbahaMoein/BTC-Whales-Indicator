import psycopg as pg
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt


def generate_totalbalance_charts(connection, cursor, timeframe: int):
    klines = cursor.execute(
        "SELECT time, close FROM public.klines WHERE MOD(time, %s) = 0 ORDER BY time ASC", (timeframe,)).fetchall()
    df = pd.DataFrame(columns=['time', 'total_balance', 'btc_price'])
    for kline in tqdm(klines):
        timestamp = kline[0]
        btcprice = kline[1]
        totalbalance = cursor.execute(
            "SELECT SUM(balance_btc) FROM public.historicalwalletbalance WHERE (starttime <= %s AND endtime >= %s AND address IN (SELECT address FROM public.wallets WHERE (balance_price_correlation > 0 AND balance_price_correlation != 'NaN')))", (timestamp, timestamp)).fetchall()[0][0]
        new_row = pd.Series({'time': timestamp, 'total_balance': totalbalance, 'btc_price': btcprice})
        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)

    df['time'] = pd.to_datetime(df['time'], unit='ms')
    
    df['level0'] = 0
    df['level1'] = 25000
    df['balance_trend'] = df['total_balance'].ewm(span=7).mean() - df['total_balance'].ewm(span=28).mean()
    exportingdf = pd.DataFrame(columns= ['date', 'time', 'open', 'high', 'low', 'close', ])
    exportingdf['date'] = df['time']
    exportingdf['time'] = ['00:00:00'] * len(df)
    exportingdf['open'] = df['balance_trend']
    exportingdf['high'] = df['balance_trend']
    exportingdf['low'] = df['balance_trend']
    exportingdf['close'] = df['balance_trend']
    exportingdf.to_csv('data.csv', index= False)

    plt.figure()

    plt.subplot(311)
    plt.plot(df['time'], df['btc_price'])
    plt.yscale('log')
    plt.title('Price')
    plt.grid(True)

    plt.subplot(312)
    plt.plot(df['time'], df['total_balance'])
    plt.yscale('log')
    plt.title('Total Balance')
    plt.grid(True)

    plt.subplot(313)
    plt.plot(df['time'], df['balance_trend'])
    plt.plot(df['time'], df['level0'], color='black')
    plt.plot(df['time'], df['level1'], color='black')
    plt.yscale('linear')
    plt.title('Total Balance Trend')
    plt.grid(True)

    plt.show()


#with pg.connect("dbname = whales user = postgres password = NURAFIN") as connection:
#    cursor = connection.cursor()
#    generate_totalbalance_charts(connection, cursor, 24*60*60*1000)