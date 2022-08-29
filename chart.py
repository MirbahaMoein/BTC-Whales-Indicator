import matplotlib.pyplot as plt
import psycopg as pg
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np
from datetime import datetime


def connect_db():
    global connection, cursor
    connection = pg.connect(
        "dbname = NURAFIN user = postgres password = NURAFIN")
    cursor = connection.cursor()


def get_table():
    connect_db()
    table = cursor.execute(
        "SELECT * FROM public.historicalbalance ORDER BY time ASC").fetchall()
    connection.close()
    return table


df = pd.DataFrame(
    get_table(), columns=['time', 'totalbalance100_btc', 'totalbalance500_btc', 'totalbalance1000_btc' , 'btcprice'])

timelist = np.array(df['time'].to_list()).reshape(-1, 1)

balancelist100 = np.array(df['totalbalance100_btc'].to_list())
balancelist500 = np.array(df['totalbalance500_btc'].to_list())
balancelist1000 = np.array(df['totalbalance1000_btc'].to_list())

model100 = LinearRegression().fit(timelist, balancelist100)
model500 = LinearRegression().fit(timelist, balancelist500)
model1000 = LinearRegression().fit(timelist, balancelist1000)

regressionline100 = model100.predict(timelist)
regressionline500 = model100.predict(timelist)
regressionline1000 = model100.predict(timelist)

df['regressionpoints100'] = regressionline100
df['regressionpoints500'] = regressionline500
df['regressionpoints1000'] = regressionline1000

df['balance100movingaverage'] = df['totalbalance100_btc'].ewm(span=10).mean()
df['balance500movingaverage'] = df['totalbalance500_btc'].ewm(span=10).mean()
df['balance1000movingaverage'] = df['totalbalance1000_btc'].ewm(span=10).mean()

for i in range(len(df)):
    df['time'][i] = datetime.fromtimestamp(df['time'][i]/1000)

fig, (ax1, ax2) = plt.subplots(2)

ax1.set_xlabel('timestamp')
ax1.set_ylabel('total balance (btc) - EMA', color='green')

ax1twin = ax1.twinx()
ax1twin.plot(df['time'], df['totalbalance100_btc'], color='green')
ax1twin.plot(df['time'], df['totalbalance500_btc'], color='blue')
ax1twin.plot(df['time'], df['totalbalance1000_btc'], color='yellow')

ax1.plot(df['time'], df['btcprice'], color='red')

ax2.plot(df['time'], df['totalbalance100_btc'] - df['balance100movingaverage'], color='green')
ax2.plot(df['time'], df['totalbalance500_btc'] - df['balance500movingaverage'], color='blue')
ax2.plot(df['time'], df['totalbalance1000_btc'] - df['balance1000movingaverage'], color='yellow')

ax1.set_ylabel('btc price', color='red')
ax1.set_yscale('log')

plt.grid('x')

plt.show()
