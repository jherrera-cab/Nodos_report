import pandas as pd

def calculate_goal(df_merge=None):
    df_goal=pd.read_csv(r'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\master_aux\master_aux_df\df_result_loguin.csv')
    df_result_day=pd.read_csv(r'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\merge\merge_df\df_merge_day.csv')