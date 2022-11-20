import psycopg as pg
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt


def generate_df(cursor, timeframe, fastema, slowema, corrthreshold, start, end):
    klines = cursor.execute("SELECT time, close FROM public.klines WHERE (MOD(time, %s) = 0 AND time >= %s AND time <= %s) ORDER BY time ASC", (timeframe, start, end)).fetchall()
    df = pd.DataFrame(columns=['time', 'total_balance', 'btc_price'])
    for kline in tqdm(klines):
        timestamp = kline[0]
        btcprice = kline[1]
        totalbalance = cursor.execute("SELECT SUM(balance_btc) FROM public.historicalwalletbalance WHERE (starttime <= %s AND endtime >= %s AND address IN (SELECT address FROM public.wallets WHERE (balance_price_correlation > %s AND balance_price_correlation != 'NaN')))", (timestamp, timestamp, corrthreshold)).fetchall()[0][0]
        new_row = pd.Series({'time': timestamp, 'total_balance': totalbalance, 'btc_price': btcprice})
        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df['balance_trend'] = df['total_balance'].ewm(span=fastema).mean() - df['total_balance'].ewm(span=slowema).mean()
    return df


def save_feather(df, corrmethod, corrtimeframe, corrthreshold, charttimeframe, lag, fastema, slowema):
    corrtf = str(int(corrtimeframe / 1000 / 60)) + 'mins'
    chtf = str(int(charttimeframe / 1000 / 60)) + 'mins'
    df = df.drop(['btc_price'], axis=1)
    df = df.sort_values(by= 'time', ignore_index=True)
    df.to_feather("./indicatordatasets/data-" + corrmethod + "-corrtf-" + corrtf + "-chtf-" + chtf + "-emaspans-" + str(fastema) + "," + str(slowema) + "-corrthreshold-" + str(corrthreshold) + '-' + str(lag) + ".ftr")


def export_to_mt5(df):
    exportingdf = pd.DataFrame(columns= ['date', 'time', 'open', 'high', 'low', 'close', ])
    exportingdf['date'] = df['time']
    exportingdf['time'] = ['00:00:00'] * len(df)
    exportingdf['open'] = df['balance_trend']
    exportingdf['high'] = df['balance_trend']
    exportingdf['low'] = df['balance_trend']
    exportingdf['close'] = df['balance_trend']
    exportingdf.to_csv('data.csv', index= False)


def generate_charts(df):
    
    df['level0'] = 0
    df['level1'] = 25000

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
    #plt.plot(df['time'], df['level1'], color='black')
    plt.yscale('linear')
    plt.title('Total Balance Trend')
    plt.grid(True)

    plt.show()