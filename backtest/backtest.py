import pandas as pd
from pathlib import Path
import os

def readfiles():
    scriptpath = Path("./")
    mainfolderpath = scriptpath.parent.absolute()
    datafolderpath = mainfolderpath / "indicatordatasets"
    fileslist = os.listdir(datafolderpath)
    filesread = {}
    for filename in fileslist:
        df = pd.read_feather(str(datafolderpath) + "\\" + filename)
        filesread[filename.split(".")[0]] = df
    return filesread
