from pathlib import Path
import pandas as pd
import os
from print_test import print_test
from data.procesed.save_df_query import save_query


def merge_df_summary():
    
    def read_df_summary(folder=None):
        raiz=Path(__file__).resolve().parents[1]
        path_folder= os.path.join(raiz, folder)
        
        df_summary={}
        files=os.listdir(path_folder)
        for file in files:
            full_path=os.path.join(path_folder, file)
            df_summary[file]=pd.read_csv(full_path)
        
        return df_summary
    
    def merge_summary_df(dfs_gestion=None, dfs_promises=None):
        keys_to_merge=['count_promises', 'ticket','ticket_medio', 'llave']
        
        df_merge_day=pd.merge(dfs_gestion['summary_gestion_day.csv'], 
                              dfs_promises['summary_promises_day.csv'][keys_to_merge], 
                              on= 'llave', 
                              how='left')
        df_merge_month=pd.merge(dfs_gestion['summary_gestion_month.csv'], 
                                dfs_promises['summary_promises_month.csv'][keys_to_merge], 
                                on= 'llave', 
                                how='left')
        
        df_merge_month_operation=pd.merge(dfs_gestion['summary_gestion_operation_month.csv'], 
                                          dfs_promises['summary_promises_operation_month.csv'][keys_to_merge], 
                                          on= 'llave', 
                                          how='left')
        
        dfs_merge={
            'df_merge_day' : df_merge_day,
            'df_merge_month' : df_merge_month,
            'df_merge_month_operation': df_merge_month_operation
        }    
        
        for df_name, df in dfs_merge.items():
            name_merge = df_name
            save_query(df=df, name=name_merge, folder=r'procesed\merge\merge_df')
        
        return(dfs_merge)
    
    folder_gestion= r'gestiones\df_gestiones'
    folder_promises= r'promises\df_promises'
    dfs_gestion_summary=read_df_summary(folder_gestion)
    dfs_promises_summary=read_df_summary(folder_promises)
        
    dfs_merge=merge_summary_df(dfs_gestion=dfs_gestion_summary, dfs_promises=dfs_promises_summary)
    
    

    
