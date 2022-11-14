import pandas as pd
from pathlib import Path
import os
from datetime import datetime
import psycopg as pg
from tqdm import tqdm
import numpy as np
pd.options.mode.chained_assignment = None

def readfiles() -> dict:
    scriptpath = Path("./")
    mainfolderpath = scriptpath.parent.absolute()
    datafolderpath = mainfolderpath / "indicatordatasets"
    fileslist = os.listdir(datafolderpath)
    filesread = {}
    for filename in fileslist:
        df = pd.read_feather(str(datafolderpath) + "\\" + filename)
        filesread[filename.replace('.ftr','')] = df
    return filesread


def backtest(signals, pricedf):
    entries = signals["entries"]
    exits = signals["exits"]
    timeframe = int((pricedf['time'][len(pricedf)-1] - pricedf['time'][len(pricedf)- 2]).total_seconds() * 1000)
    startingbalance = 100000
    balance = startingbalance
    newdf = pd.DataFrame(columns = ['time', 'close', 'pctchange', 'position', 'systemreturn', 'balance'])
    for index in range(len(pricedf)):
        time = pricedf['time'][index]
        close = pricedf['close'][index]
        pctchange = pricedf['pctchange'][index]
        newrow = pd.Series({'time': time, 'close': close, 'pctchange':pctchange, 'position':0, 'systemreturn':0, 'balance': balance})
        newdf = pd.concat([newdf, newrow.to_frame().T], ignore_index= True)
    
    if len(entries) - len(exits) == 1:
        exits.append(datetime.now())
    
    newdf['time'] = newdf['time'].values.astype(np.int64)/1000000

    for index in range(len(entries)):
        entrytime = int(entries[index][0].timestamp() *1000)
        exittime = int(exits[index].timestamp() *10000)        
        newdf['position'][newdf.index[newdf['time'].isin(range(entrytime, exittime + 1, timeframe))]] = entries[index][1]

    for index in range(1, len(newdf)):
        newdf['systemreturn'][index] = newdf['position'][index - 1] * newdf['pctchange'][index]
        newdf['balance'][index] = newdf['systemreturn'][index] * newdf['balance'][index - 1] + newdf['balance'][index - 1]

    if len(entries) == 0:
        sharperatio = 0
    else:
        sharperatio = newdf['systemreturn'].mean() / newdf['systemreturn'].std()

    accumulativereturn = ((newdf['balance'][len(newdf) - 1] / startingbalance) - 1) * 100
    newdf['highvalue'] = newdf['balance'].cummax()
    newdf['drawdown'] = 1 - (newdf['balance'] / newdf['highvalue'])
    maxdrawdown = newdf['drawdown'].max()

    return (accumulativereturn, sharperatio, maxdrawdown)


def generate_signals(df, lowerband, higherband, starttime, endtime):
    entries = []
    exits = []
    underlowerbandflag = 0
    overhigherbandflag = 0
    
    if lowerband >= 0:
        underlowerbandflag = 1
        overhigherbandflag = 0
    elif higherband <= 0:
        overhigherbandflag = 1
        underlowerbandflag = 0
    else:
        underlowerbandflag = 1
        overhigherbandflag = 1

    for index in range(1, len(df)):
        if df['time'][index] >= starttime and df['time'][index] <= endtime:
            if df["balance_trend"][index] > higherband and df["balance_trend"][index - 1] < higherband and underlowerbandflag == 1:
                overhigherbandflag = 1
                underlowerbandflag = 0
                if len(entries) == len(exits) + 1:
                    exits.append((df['time'][index]))
                entries.append((df['time'][index], 1))
            if df["balance_trend"][index] < lowerband and df["balance_trend"][index - 1] > lowerband and overhigherbandflag == 1:
                overhigherbandflag = 0
                underlowerbandflag = 1
                if len(entries) == len(exits) + 1:
                    exits.append((df['time'][index]))
                entries.append((df['time'][index], -1))
    if len(entries) - len(exits) == 1:
        exits.append(datetime.now())
    signals = {'entries': entries, 'exits': exits}
    return signals


def getpricedf(indicatordf):
    starttime = indicatordf['time'][0].timestamp() * 1000
    endtime = indicatordf['time'][len(indicatordf)-1].timestamp() * 1000
    timeframe = int((indicatordf['time'][len(indicatordf)-1] - indicatordf['time'][len(indicatordf)-2]).total_seconds() * 1000)
    df = pd.DataFrame(columns = ['time', 'close', 'pctchange'])
    with pg.connect("dbname = whales user = postgres password = NURAFIN") as connection:
        cursor = connection.cursor()
        klines = cursor.execute("SELECT time, close FROM public.klines WHERE (time >= %s AND time <= %s AND MOD(time, %s) = 0) ORDER BY time ASC", (starttime, endtime, timeframe)).fetchall()
    for index in range(1, len(klines)):
        time = klines[index][0]
        close = klines[index][1]
        pctchange = (klines[index][1] / klines[index - 1][1]) - 1
        newrow = pd.Series({'time': time, 'close': close, 'pctchange': pctchange})
        df = pd.concat([df, newrow.to_frame().T], ignore_index= True)
    df['time'] = pd.to_datetime(df['time'], unit= 'ms')
    return df


def main():
    files = readfiles()
    evaldf = pd.DataFrame(columns= ['filename', 'lowerband', 'higherband', 'accumulativeprofit', 'sharperatio', 'maxdrawdown' 'numberoftrades']) 
    for filename in tqdm(list(files)[1:], position= 0):
        df = files[filename]
        calcdf = getpricedf(df)
        maxindicatorvalue = int(df.loc[df["balance_trend"].idxmax()]["balance_trend"])
        minindicatorvalue = int(df.loc[df["balance_trend"].idxmin()]["balance_trend"])
        numberofsteps = 20
        stepsize = int((maxindicatorvalue - minindicatorvalue) / numberofsteps)
        for lowerband in tqdm(range(minindicatorvalue + stepsize, maxindicatorvalue - 2 * stepsize, stepsize), leave= False, position= 1):
            for higherband in tqdm(range(lowerband + stepsize, maxindicatorvalue - stepsize, stepsize), leave= False, position= 2):
                signals = generate_signals(df, lowerband, higherband, datetime(2017, 8, 1), datetime(2023, 1, 1))
                testresults = backtest(signals, calcdf)
                accprofit = testresults[0]
                sharperatio = testresults[1]
                maxdd = testresults[2]
                numberoftrades = len(signals['entries'])
                newrow = pd.Series({'filename' : filename, 'lowerband': lowerband, 'higherband': higherband, 'accumulativeprofit': accprofit, 'sharperatio': sharperatio, 'maxdrawdown': maxdd, 'numberoftrades': numberoftrades})
                evaldf = pd.concat([evaldf, newrow.to_frame().T], ignore_index= True)
    evaldf = evaldf.sort_values(by= 'accumulativeprofit', ascending= False, ignore_index= True)
    print(evaldf)
    evaldf.to_excel("Evaluation.xlsx")


main()