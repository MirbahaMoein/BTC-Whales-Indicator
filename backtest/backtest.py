import pandas as pd
from pathlib import Path
import os
from datetime import datetime
import psycopg as pg
from tqdm import tqdm
import numpy as np
from matplotlib import pyplot as plt
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


def backtestfunc(signals, df):
    entries = signals["entries"]
    exits = signals["exits"]

    startingbalance = 100000
    balance = startingbalance

    df['position'] = 0
    df['systemreturn'] = 0
    df['balance'] = balance

    for dfindex in range(len(df)):
        dftime = df['time'][dfindex]
        for signalindex in range(len(entries)):
            entrytime = entries[signalindex][0]
            entrydirection = entries[signalindex][1]
            exittime = exits[signalindex]
            if dftime > entrytime and dftime <= exittime:
                df['position'][dfindex] = entrydirection
                break

    for index in range(1, len(df)):
        df['systemreturn'][index] = df['position'][index] * df['pctchange'][index]
        df['balance'][index] = df['systemreturn'][index] * df['balance'][index - 1] + df['balance'][index - 1]

    if len(entries) == 0:
        sharperatio = 0
    else:
        sharperatio = df['systemreturn'].mean() / df['systemreturn'].std()

    accumulativereturn = ((df['balance'][len(df) - 1] / startingbalance) - 1) * 100
    df['highvalue'] = df['balance'].cummax()
    df['drawdown'] = 1 - (df['balance'] / df['highvalue'])
    maxdrawdown = df['drawdown'].max() * 100

    #df.plot(x= 'time', y= 'balance')
    #df.plot(x= 'time', y= 'drawdown')
    #plt.show()

    return (df, accumulativereturn, sharperatio, maxdrawdown)


def generate_signals(df, lowerband, higherband):
    endtime = df['time'][len(df) - 1]
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
        exits.append(endtime)
    signals = {'entries': entries, 'exits': exits}
    return signals


def getpricedf(indicatordf):
    start = indicatordf['time'][0]
    end = indicatordf['time'][len(indicatordf) - 1]
    timeframe = int(indicatordf['time'][len(indicatordf)-1] - indicatordf['time'][len(indicatordf)-2])
    df = pd.DataFrame(columns = ['time', 'close', 'pctchange'])
    with pg.connect("dbname = whales user = postgres password = NURAFIN") as connection:
        cursor = connection.cursor()
        klines = cursor.execute("SELECT time, close FROM public.klines WHERE (time >= %s AND time <= %s AND MOD(time, %s) = 0) ORDER BY time ASC", (start, end, timeframe)).fetchall()
    for index in range(1, len(klines)):
        time = klines[index][0]
        close = klines[index][1]
        pctchange = (klines[index][1] / klines[index - 1][1]) - 1
        newrow = pd.Series({'time': time, 'close': close, 'pctchange': pctchange})
        df = pd.concat([df, newrow.to_frame().T], ignore_index= True)
    return df


def divideindicator(df, start, end):
    df['time'] = df['time'].values.astype(np.int64)/1000000
    for index in range(len(df)):
        if df['time'][index] < start or df['time'][index] > end:
            df.drop(index, inplace=True)
    return df


def main():
    files = readfiles()
    evaldf = pd.DataFrame(columns= ['filename', 'lowerband', 'higherband', 'accumulativeprofit', 'sharperatio', 'maxdrawdown' 'numberoftrades']) 
    periodstart = datetime(2021, 5, 1).timestamp()*1000
    periodend = datetime(2022, 1, 1).timestamp()*1000
    for filename in tqdm(list(files), position= 0):
        df = files[filename]
        df = divideindicator(df, periodstart, periodend)
        calcdf = getpricedf(df)
        maxindicatorvalue = int(df.loc[df["balance_trend"].idxmax()]["balance_trend"])
        minindicatorvalue = int(df.loc[df["balance_trend"].idxmin()]["balance_trend"])
        numberofsteps = 20
        stepsize = int((maxindicatorvalue - minindicatorvalue) / numberofsteps)
        for lowerband in tqdm(range(minindicatorvalue + stepsize, maxindicatorvalue - 2 * stepsize, stepsize), leave= False, position= 1):
            for higherband in tqdm(range(lowerband + stepsize, maxindicatorvalue - stepsize, stepsize), leave= False, position= 2):
                signals = generate_signals(df, lowerband, higherband)
                testresults = backtestfunc(signals, calcdf)
                backtestdf = testresults[0]
                accprofit = testresults[1]
                sharperatio = testresults[2]
                maxdd = testresults[3]
                numberoftrades = len(signals['entries'])
                newrow = pd.Series({'filename' : filename, 'lowerband': lowerband, 'higherband': higherband, 'accumulativeprofit': accprofit, 'sharperatio': sharperatio, 'maxdrawdown': maxdd, 'numberoftrades': numberoftrades})
                evaldf = pd.concat([evaldf, newrow.to_frame().T], ignore_index= True)
    evaldf = evaldf.sort_values(by= 'accumulativeprofit', ascending= False, ignore_index= True)
    print(evaldf)
    evaldf.to_excel("Evaluation.xlsx")


main()