import pandas as pd
from pathlib import Path
import os

scriptpath = Path("./")
folderpath = scriptpath.parent.absolute()
fileslist = os.listdir(folderpath)
filestoread = []
for filename in fileslist:
    if len(filename.split(".")) == 2:
        extension = filename.split(".")[1]
        print(extension)
        if extension == "ftr":
            df = pd.read_feather(str(folderpath) + "\\" + filename)
            
