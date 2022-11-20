from backtest import getpricedf, generate_signals, backtestfunc, divideindicator
from pathlib import Path
import pandas as pd
from datetime import datetime
from tqdm import tqdm

selectedstrategies = [{'filename': 'data-pearson-corrtf-240mins-chtf-240mins-emaspans-2,4-corrthreshold-0', 'lowerband': -1036, 'higherband': 728}, {'filename': 'data-pearson-corrtf-240mins-chtf-240mins-emaspans-8,32-corrthreshold-0.121986315', 'lowerband': 174, 'higherband': 1214}, {'filename': 'data-pearson-corrtf-240mins-chtf-240mins-emaspans-8,16-corrthreshold-0.121986315', 'lowerband': 26, 'higherband': 535}]

df = pd.read_excel("Evaluation.xlsx", 'Sort')
for index in range(len(df)):
    if df['test'][index] == 1:
        selectedstrategies.append({'filename': df['filename'][index], 'lowerband': df['lb'][index], 'higherband': df['hb'][index]})


scriptpath = Path("./")
mainfolderpath = scriptpath.parent.absolute()
datafolderpath = mainfolderpath / "indicatordatasets"
evaldf = pd.DataFrame(columns= ['filename', 'lowerband', 'higherband', 'accumulativeprofit', 'sharperatio', 'maxdrawdown' 'numberoftrades']) 
for dict in tqdm(selectedstrategies):
    filename = dict['filename'] + '.ftr'
    lowerband = dict['lowerband']
    higherband = dict['higherband']
    df = pd.read_feather(str(datafolderpath) + "\\" + filename)
    divideddf = divideindicator(df, datetime(2022, 1, 1).timestamp()*1000, datetime(2022, 5, 1).timestamp()*1000)
    divideddf = divideddf.reset_index(drop=True)    
    calcdf = getpricedf(divideddf)
    signals = generate_signals(divideddf, lowerband, higherband)
    testresults = backtestfunc(signals, calcdf)
    backtestdf = testresults[0]
    backtestdf.to_excel("backtestdfs/" + dict['filename'] + ',' +  str(lowerband) + ',' + str(higherband) + "-dataframe.xlsx")
    accprofit = testresults[1]
    sharperatio = testresults[2]
    maxdd = testresults[3]
    numberoftrades = len(signals['entries'])
    newrow = pd.Series({'filename' : filename, 'lowerband': lowerband, 'higherband': higherband, 'accumulativeprofit': accprofit, 'sharperatio': sharperatio, 'maxdrawdown': maxdd, 'numberoftrades': numberoftrades})
    evaldf = pd.concat([evaldf, newrow.to_frame().T], ignore_index= True)

evaldf = evaldf.sort_values(by= 'accumulativeprofit', ascending= False, ignore_index= True)
print(evaldf)
evaldf.to_excel("Evaluation on test.xlsx")
