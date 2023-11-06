import os
import pandas as pd
from pathlib import Path


def read_dfs():
    def read_acw():
        raiz=Path(__file__).resolve().parents[2]
        path_acw=os.path.join(raiz,r'data\procesed\gestiones\df_gestiones')
        file_acw=os.listdir(path_acw)
        dic_acw_week={}
        dic_acw={}
        
        for file in file_acw:
            name_file=os.path.splitext(os.path.basename(file))
            path_file=os.path.join(path_acw, file)
            df = pd.read_csv(path_file)
            if "summary_acw_weeks_" in file:
                dic_acw_week[name_file[0]]=df
            else:
                dic_acw[name_file[0]]=df
                
        return dic_acw, dic_acw_week
        
    return read_acw()
read_dfs()    