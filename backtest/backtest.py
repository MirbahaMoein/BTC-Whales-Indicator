import pandas as pd
from pathlib import Path
import os
from datetime import datetime
import psycopg as pg


def readfiles() -> dict:
    scriptpath = Path("./")
    mainfolderpath = scriptpath.parent.absolute()
    datafolderpath = mainfolderpath / "indicatordatasets"
    fileslist = os.listdir(datafolderpath)
    filesread = {}
    for filename in fileslist:
        df = pd.read_feather(str(datafolderpath) + "\\" + filename)
        filesread[filename.split(".")[0]] = df
    return filesread


def optimize_indicator_thresholds(df: pd.DataFrame):
    maxindicatorvalue = df.loc[df["balance_trend"].idxmax()]["balance_trend"]
    minindicatorvalue = df.loc[df["balance_trend"].idxmin()]["balance_trend"]
    numberofsteps = 10
    stepsize = int((maxindicatorvalue - minindicatorvalue) / numberofsteps)
    for lowerband in range(minindicatorvalue + stepsize, maxindicatorvalue - 2 * stepsize, stepsize):
        for higherband in range(lowerband + stepsize, maxindicatorvalue - stepsize, stepsize):
            signals = generate_signals(df, lowerband, higherband)
            stats = backtest(signals)


def backtest(signals):
    entries = signals["entries"]
    exits = signals["exits"]
    startingbalance = 100000
    balance = startingbalance
    if len(entries) - len(exits) <= 1:
        with pg.connect("dbname = whales user = postgres password = NURAFIN") as connection:
            cursor = connection.cursor()
            for i in range(len(entries)):
                entrytime = entries[i][0].timestamp() *1000 
                exittime = exits[i].timestamp() *1000
                entryprice = cursor.execute("SELECT close FROM public.klines ORDER BY ABS(time - %s) ASC LIMIT 1", (entrytime,)).fetchall()[0][0]
                exitprice = cursor.execute("SELECT close FROM public.klines ORDER BY ABS(time - %s) LIMIT 1", (exittime,)).fetchall()[0][0]
                signaldirection = entries[i][1]
                #print(entrytime, entryprice)
                #print(exittime, exitprice)
                #print((exitprice - entryprice) / entryprice)
                print(balance)
                balance = ((exitprice / entryprice) ** signaldirection) * balance
                profitpercent = int(((exitprice / entryprice) ** signaldirection - 1) * 10000) /100
                print(entryprice, signaldirection, exitprice)
                print(profitpercent, balance)
            print("Accumulative Profit:" + str(int((balance - startingbalance)/startingbalance*10000)/100))
    else:
        print("entries more than exits+1")


def generate_signals(df, lowerband, higherband):
    entries = []
    exits = []
    underlowerbandflag = 1
    overhigherbandflag = 0
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
        exits.append(datetime.now())
    signals = {'entries': entries, 'exits': exits}
    print(signals)
    return signals


backtest(generate_signals(list(readfiles().values())[0],0,25000))
