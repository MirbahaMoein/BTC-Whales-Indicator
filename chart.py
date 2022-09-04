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


def get_table(cursor):
    table = cursor.execute(
        "SELECT * FROM public.historicalbalance ORDER BY time ASC").fetchall()
    df = pd.DataFrame(table, columns=['time', 'totalbalance_btc', 'totalbalance100_btc', 'totalbalance500_btc', 'totalbalance1000_btc', 'btcprice'])
    return df


def add_regression(df):
    timelist = np.array(df['time'].to_list()).reshape(-1, 1)
    balancelisttotal = np.array(df['totalbalance_btc'].to_list())
    balancelist100 = np.array(df['totalbalance100_btc'].to_list())
    balancelist500 = np.array(df['totalbalance500_btc'].to_list())
    balancelist1000 = np.array(df['totalbalance1000_btc'].to_list())
    modeltotal = LinearRegression().fit(timelist, balancelisttotal)
    model100 = LinearRegression().fit(timelist, balancelist100)
    model500 = LinearRegression().fit(timelist, balancelist500)
    model1000 = LinearRegression().fit(timelist, balancelist1000)
    df['regressiontotal'] = modeltotal.predict(timelist)
    df['regression100'] = model100.predict(timelist)
    df['regression500'] = model500.predict(timelist)
    df['regression1000'] = model1000.predict(timelist)
    return df


def add_moving_averages(df, span):
    df['slowtotalbalanceMA'] = df['totalbalance_btc'].ewm(
        span=span*15).mean()
    df['fasttotalbalanceMA'] = df['totalbalance_btc'].ewm(
        span=span).mean()
    df['slow100balanceMA'] = df['totalbalance100_btc'].ewm(
        span=span*15).mean()
    df['fast100balanceMA'] = df['totalbalance100_btc'].ewm(
        span=span).mean()
    df['slow500balanceMA'] = df['totalbalance500_btc'].ewm(
        span=span*15).mean()
    df['fast500balanceMA'] = df['totalbalance500_btc'].ewm(
        span=span).mean()
    df['slow1000balanceMA'] = df['totalbalance1000_btc'].ewm(
        span=span*15).mean()
    df['fast1000balanceMA'] = df['totalbalance1000_btc'].ewm(
        span=span).mean()
    return df


def convert_to_datetime(df):
    for i in range(len(df)):
        df['time'][i] = datetime.fromtimestamp(df['time'][i]/1000)
    return df


def show_chart(df):
    axes = plt.subplots(2, 3)[1]

    axes[0,0].set_xlabel('time')
    axes[0,0].set_ylabel('price')
    axes[0,0].plot(df['time'], df['btcprice'], color='green')
    axes[0,0].set_yscale('log')

    axes[0,1].set_xlabel('time')
    axes[0,1].set_ylabel('total balance(100)')
    axes[0,1].plot(df['time'], df['totalbalance100_btc'], color='black')
    axes[0,1].set_yscale('log')

    axes[0,2].set_xlabel('time')
    axes[0,2].set_ylabel('total balance(500)')
    axes[0,2].plot(df['time'], df['totalbalance500_btc'], color='black')
    axes[0,2].set_yscale('log')

    axes[1,1].set_xlabel('time')
    axes[1,1].set_ylabel('total balance(1000)')
    axes[1,1].plot(df['time'], df['totalbalance1000_btc'], color='black')
    axes[1,1].set_yscale('log')

    axes[1,2].set_xlabel('time')
    axes[1,2].set_ylabel('total balance')
    axes[1,2].plot(df['time'], df['totalbalance_btc'], color='black')
    axes[1,2].set_yscale('log')

    axes[1,0].set_xlabel('time')
    axes[1,0].set_ylabel('Balance Trend Index')
    axes[1,0].plot(df['time'], df['totalbalance100_btc'] - df['slow100balanceMA'], color='yellow')
    axes[1,0].plot(df['time'], [0] * len(df), color = 'black')

    plt.show()


def main():
    (connection, cursor) = connect_db()
    df = get_table(cursor)
    connection.close()
    df = add_regression(df)
    df = add_moving_averages(df, 8)
    df = convert_to_datetime(df)
    show_chart(df)


main()
