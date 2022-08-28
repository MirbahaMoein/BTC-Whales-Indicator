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
    get_table(), columns=['time', 'totalbalancebtc', 'price', 'totalbalanceusd'])

timelist = np.array(df['time'].to_list()).reshape(-1, 1)
balancelist = np.array(df['totalbalancebtc'].to_list())

model = LinearRegression().fit(timelist, balancelist)

regressionline = model.predict(timelist)

df['regressionpoints'] = regressionline

df['balancemovingaverage'] = df['totalbalancebtc'].ewm(span = 10).mean()

for i in range(len(df)):
    df['time'][i] = datetime.fromtimestamp(df['time'][i]/1000)

fig, (ax1, ax2) = plt.subplots(2)


ax2.plot(df['time'], df['totalbalancebtc'], color = 'green', )
ax2.plot(df['time'], [0] * len(df), color = 'black')

ax1twin = ax1.twinx()
ax1twin.plot(df['time'], df['totalbalancebtc'] - df['balancemovingaverage'], color = 'green')

#ax1.plot(df['time'], [50000] * len(df), color = 'black')
#ax1.plot(df['time'], [-50000] * len(df), color = 'black')
ax1.set_xlabel('timestamp')
ax1.set_ylabel('total balance (btc) - EMA', color='green')
#ax1.set_yscale('log')


ax2.plot(df['time'], df['price'], color='red')
ax2.set_ylabel('btc price', color='red')
ax2.set_yscale('log')
plt.grid('x')

plt.show()
