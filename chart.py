import matplotlib.pyplot as plt
import psycopg as pg
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np
from datetime import datetime

pd.options.mode.chained_assignment = None


def connect_db():
    connection = pg.connect(
        "dbname = NURAFIN user = postgres password = NURAFIN")
    cursor = connection.cursor()
    return (connection, cursor)


def get_table(cursor, mode):
    if mode == 100 or mode == 500 or mode == 1000:
        table = cursor.execute(
            "SELECT time, totalbalance{}_btc, btcprice FROM public.historicalbalance ORDER BY time ASC".format(str(mode))).fetchall()
        status = True
    else:
        table = []
        status = False
    df = pd.DataFrame(table, columns=['time', 'totalbalance_btc', 'btcprice'])
    return df, status


def add_regression(df):
    timelist = np.array(df['time'].to_list()).reshape(-1, 1)
    balancelist = np.array(df['totalbalance_btc'].to_list())
    model = LinearRegression().fit(timelist, balancelist)
    df['regression'] = model.predict(timelist)
    return df


def add_moving_averages(df, span):
    df['slowbalancemovingaverage'] = df['totalbalance_btc'].ewm(
        span=span).mean()
    df['fastbalancemovingaverage'] = df['totalbalance_btc'].ewm(
        span=span * 15).mean()
    return df


def convert_to_datetime(df):
    for i in range(len(df)):
        df['time'][i] = datetime.fromtimestamp(df['time'][i]/1000)
    return df


def show_chart(df):
    (ax1, ax2) = plt.subplots(2)[1]

    ax1.set_xlabel('time')
    

    ax1.set_ylabel('btc price', color='red')
    ax1.set_yscale('log')

    ax1.plot(df['time'], df['btcprice'], color='red')

    ax2.plot(df['time'], df['fastbalancemovingaverage'] -
             df['slowbalancemovingaverage'], color='green')
    
    ax2.plot(df['time'], [0] * len(df), color= 'gray')
    ax2.set_ylabel('balance fast EMA - balance slow EMA', color='green')

    ax1twin = ax1.twinx()
    ax1twin.plot(df['time'], df['totalbalance_btc'], color='green')

    plt.grid('x')
    plt.show()


def main():
    (connection, cursor) = connect_db()
    df, status = get_table(cursor, mode=1000)
    connection.close()
    if status:
        df = add_regression(df)
        df = add_moving_averages(df, 24)
        df = convert_to_datetime(df)
        show_chart(df)


main()
