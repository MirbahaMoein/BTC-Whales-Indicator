import os
from matplotlib import pyplot as plt
import pandas as pd
from pathlib import Path
import re

def readfiles() -> dict:
    scriptpath = Path("./")
    mainfolderpath = scriptpath.parent.absolute()
    datafolderpath = mainfolderpath / "backtestdfs"
    fileslist = os.listdir(datafolderpath)
    filesread = {}
    for filename in fileslist:
        df = pd.read_excel(str(datafolderpath) + "\\" + filename, index_col= 0)
        filesread[filename.replace('.xlsx','')] = df
    return filesread

files = readfiles()

for filename in list(files):
    higherband = float(re.findall('[^,]+(?=-dataframe)', filename)[0])
    try:
        lowerband = float(re.findall('[^,]+(?=' + ',' + str(higherband) + '-dataframe)', filename)[0])
    except:
        lowerband = float(re.findall('[^,]+(?=' + ',' + str(int(higherband)) + '-dataframe)', filename)[0])
    df = files[filename]
    df['time'] = pd.to_datetime(df['time']*1000000)
    
    plt.figure()

    plt.subplot(211)
    plt.plot(df['time'], df['close'])
    #plt.yscale('log')
    plt.title('Price')
    plt.grid(True)

    plt.subplot(212)
    plt.plot(df['time'], df['balance'])
    #plt.yscale('log')
    plt.title('Portfolio balance')
    plt.grid(True)

    plt.show()
