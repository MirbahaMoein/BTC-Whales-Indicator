from tqdm import tqdm
import pandas as pd
import psycopg as pg
from datetime import *
import numpy as np
pd.options.mode.chained_assignment = None


def generate_df(cursor, timeframe, fastema, slowema, corrthreshold, start, end, speriod, lperiod, lag):
    klines = cursor.execute("SELECT time, close FROM public.klines WHERE (MOD(time, %s) = 0 AND time >= %s AND time <= %s) ORDER BY time ASC", (timeframe, start, end)).fetchall()
    #balances = cursor.execute("")
    df = pd.DataFrame(columns=['time', 'total_balance', 'btc_price'])
    for kline in tqdm(klines):
        timestamp = kline[0]
        btcprice = kline[1]
        totalbalance = cursor.execute("SELECT SUM(balance_btc) FROM public.historicalwalletbalance WHERE (starttime <= %s AND endtime >= %s AND address IN (SELECT address FROM public.correlations WHERE (speriod = %s AND lperiod = %s AND lag = %s AND timeframems = %s AND periodstart = %s AND correlation > %s AND correlation != 'NaN')))", (timestamp, timestamp, speriod, lperiod, lag, timeframe, start, corrthreshold)).fetchall()[0][0]
        new_row = pd.Series({'time': timestamp, 'total_balance': totalbalance, 'btc_price': btcprice})
        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df['balance_trend'] = df['total_balance'].ewm(span=fastema).mean() - df['total_balance'].ewm(span=slowema).mean()
    return df


def getpricedf(indicatordf):
    start = int(indicatordf['time'][0])
    end = int(indicatordf['time'][len(indicatordf) - 1])
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
    df = df[(df['time'] >= start) & (df['time'] <= end)]
    df = df.sort_values(by= 'time', ignore_index= True)
    return df


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
        df['balance'][index] = (df['systemreturn'][index] + 1) * df['balance'][index - 1] 

    if len(entries) == 0:
        sharperatio = 0
    else:
        sharperatio = df['systemreturn'].mean() / df['systemreturn'].std()

    accumulativereturn = ((df['balance'][len(df) - 1] / startingbalance) - 1) * 100

    df['highvalue'] = df['balance'].cummax()
    df['drawdown'] = 1 - (df['balance'] / df['highvalue'])
    maxdrawdown = df['drawdown'].max() * 100
    
    return (df, accumulativereturn, sharperatio, maxdrawdown)


with pg.connect("dbname = whales user = postgres password = NURAFIN") as connection:
    cursor = connection.cursor()
    firstperiodstart = int(datetime(2018,1,1).timestamp()*1000)
    lastperiodstart = int(datetime(2020,1,1).timestamp()*1000)
    stepofperiodstart = int(timedelta(days= 365).total_seconds()*1000)
    evaldf = pd.DataFrame(columns= ['timeframe', 'corrcalclperiod', 'corrcalcsperiod', 'corrcalclag', 'corrthreshold', 'ilperiod', 'isperiod', 'lowerband', 'higherband', 'accumulativeprofit', 'sharperatio', 'maxdrawdown' 'numberoftrades']) 
    for correlationcalculationtimeframems in tqdm([4 * 60 * 60 * 1000, 24 * 60 * 60 * 1000], position= 0, leave= False, desc= 'timeframe'): 
        for periodstart in tqdm(range(firstperiodstart, lastperiodstart + 1, stepofperiodstart) , position = 1, leave = False, desc= 'period'):
            periodend = periodstart + int(timedelta(days= 2 * 365).total_seconds()*1000)
            for lperiod in tqdm(range(2, 31, 3), position= 2, leave= False, desc= 'lperiod'):
                for speriod in tqdm(range(1, int(lperiod / 2) + 1, 2), position = 3, leave = False, desc = 'speriod'):
                    for lag in tqdm(range(0, 31, 3), position= 4, leave= False, desc= 'lag'):
                        for slowema in range(2, 31, 3):
                            for fastema in range(1, int(lperiod / 2) + 1, 2):
                                maxcorr = cursor.execute("SELECT MAX(correlation) FROM public.correlations WHERE (speriod = %s AND lperiod = %s AND lag = %s AND timeframems = %s AND periodstart = %s)", (speriod, lperiod, lag, correlationcalculationtimeframems, periodstart)).fetchall()[0][0]
                                for corrthreshold in [0, maxcorr * 1/4, maxcorr * 2/4, maxcorr * 3/4]:    
                                    df = generate_df(cursor, correlationcalculationtimeframems, fastema, slowema, corrthreshold, periodstart, periodend + timedelta(days = 180).total_seconds()*1000, speriod, lperiod, lag)
                                    #periodend + timedelta(days = 180).total_seconds()*1000
                                    traindf = divideindicator(df, periodstart, periodend)
                                    calcdf = getpricedf(traindf)
                                    try:
                                        maxindicatorvalue = int(traindf.loc[traindf["balance_trend"].idxmax()]["balance_trend"])
                                        minindicatorvalue = int(traindf.loc[traindf["balance_trend"].idxmin()]["balance_trend"])
                                        numberofsteps = 20
                                        stepsize = int((maxindicatorvalue - minindicatorvalue) / numberofsteps)
                                        for lowerband in tqdm(range(minindicatorvalue + stepsize, maxindicatorvalue - 2 * stepsize, stepsize), leave= False, position= 1):
                                            for higherband in tqdm(range(lowerband + stepsize, maxindicatorvalue - stepsize, stepsize), leave= False, position= 2):
                                                signals = generate_signals(traindf, lowerband, higherband)
                                                testresults = backtestfunc(signals, calcdf)
                                                accprofit = testresults[1]
                                                sharperatio = testresults[2]
                                                maxdd = testresults[3]
                                                numberoftrades = len(signals['entries'])
                                                newrow = pd.Series({'timeframe' : correlationcalculationtimeframems, 'corrcalclperiod': lperiod, 'corrcalcsperiod': speriod, 'corrcalclag': lag, 'corrthreshold': corrthreshold, 'ilperiod': slowema, 'isperiod': fastema, 'lowerband': lowerband, 'higherband': higherband, 'accumulativeprofit': accprofit, 'sharperatio': sharperatio, 'maxdrawdown': maxdd, 'numberoftrades': numberoftrades})
                                                evaldf = pd.concat([evaldf, newrow.to_frame().T], ignore_index= True)
                                    except:
                                        pass
evaldf = evaldf.sort_values(by= 'accumulativeprofit', ascending= False, ignore_index= True)
print(evaldf)
evaldf.to_excel("Evaluation.xlsx") 