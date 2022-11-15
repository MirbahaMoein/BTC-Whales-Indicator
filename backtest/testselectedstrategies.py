from backtest import getpricedf, generate_signals, backtestfunc, divideindicator
from pathlib import Path
import pandas as pd
from datetime import datetime

selectedstrategies = [{'filename': 'data-pearson-corrtf-240mins-chtf-240mins-emaspans-2,8-corrthreshold-0', 'lowerband': -618, 'higherband': 4235}, {'filename': 'data-pearson-corrtf-240mins-chtf-240mins-emaspans-24,96-corrthreshold-0.14185472', 'lowerband': 2259, 'higherband': 11839}]

scriptpath = Path("./")
mainfolderpath = scriptpath.parent.absolute()
datafolderpath = mainfolderpath / "indicatordatasets"
evaldf = pd.DataFrame(columns= ['filename', 'lowerband', 'higherband', 'accumulativeprofit', 'sharperatio', 'maxdrawdown' 'numberoftrades']) 
for dict in selectedstrategies[:1]:
    filename = dict['filename'] + '.ftr'
    lowerband = dict['lowerband']
    higherband = dict['higherband']
    df = pd.read_feather(str(datafolderpath) + "\\" + filename)
    df = divideindicator(df, datetime(2022, 1, 1).timestamp()*1000, datetime(2022, 5, 1).timestamp()*1000)
    calcdf = getpricedf(df)
    signals = generate_signals(df, lowerband, higherband)
    testresults = backtestfunc(signals, calcdf)
    accprofit = testresults[0]
    sharperatio = testresults[1]
    maxdd = testresults[2]
    numberoftrades = len(signals['entries'])
    newrow = pd.Series({'filename' : filename, 'lowerband': lowerband, 'higherband': higherband, 'accumulativeprofit': accprofit, 'sharperatio': sharperatio, 'maxdrawdown': maxdd, 'numberoftrades': numberoftrades})
    evaldf = pd.concat([evaldf, newrow.to_frame().T], ignore_index= True)
evaldf = evaldf.sort_values(by= 'accumulativeprofit', ascending= False, ignore_index= True)
print(evaldf)
evaldf.to_excel("Evaluation on test.xlsx")