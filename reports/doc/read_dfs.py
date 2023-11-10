import os
import pandas as pd
from pathlib import Path
from reports.convert_list.convert import df_to_list
import os
from print_test import print_test

raiz=Path(__file__).resolve().parents[2]

def read_dfs():

    def read_file(folder=None, name_df_file=None):
        
        path=os.path.join(raiz,folder)
        file=os.listdir(path)
        dic_week={}
        dic_summary={}
        
        for file in file:
            name_file=os.path.splitext(os.path.basename(file))
            path_file=os.path.join(path, file)
            df = pd.read_csv(path_file)
            if name_df_file in file:
                dic_week[name_file[0]]=df_to_list(df)
            else:
                dic_summary[name_file[0]]=df_to_list(df)

        return dic_week, dic_summary
        
    
    var_df=[
        {
            'gestiones':{
            'folder': r'data\procesed\gestiones\df_gestiones',
            'name_df_file': "summary_acw_weeks_"
            },
            'goals':{
            'folder':r'data\procesed\goals\goals_df',
            'name_df_file':'df_kpi_report'
            },
            'aux':{
            'folder':r'data\procesed\master_aux\master_aux_df',
            'name_df_file':'df_aux_week_'
            }
        }
    ]
    
    dic_result={}
    
    for key, value in var_df[0].items():
        dic_week, dic_summary = read_file(folder=value['folder'], name_df_file=value['name_df_file'])
        dic_result[key + '_week']=(dic_week)
        dic_result[key + '_summary']=(dic_summary)
    

    return dic_result
    