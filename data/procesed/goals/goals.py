import pandas as pd
from print_test import print_test

def calculate_goal(df_merge=None):
    
    def calculate_cumplice_goals(df_result=None, df_goals=None, name_file=None):
        df_goal_merge=df_result.merge(df_goals, on='NOMBRE', how='inner')
        df_goal_merge['cumplice_gestion']=round(df_goal_merge['count'] / df_goal_merge['objetive_gestion'], 2) * 100
        df_goal_merge['cumplice_contact']=round(df_goal_merge['util_positive'] / df_goal_merge['objetive_contact'], 2) * 100
        df_goal_merge['cumplice_promises']=round(df_goal_merge['count_promises'] / df_goal_merge['objetive_promises'], 2) * 100
        
        df_goal_merge.to_excel(r'C:\Users\jherrera.FINANCREDITOS\Desktop\merge_goals.xlsx', index=False)
        
        return df_goal_merge
        
    df_goal_day=pd.read_csv(r'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\master_aux\master_aux_df\df_result_loguin_day.csv')
    df_result_day=pd.read_csv(r'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\merge\merge_df\df_merge_day.csv')
    df_result_month=pd.read_csv(r'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\merge\merge_df\df_merge_month.csv')
    df_result_month_operation=pd.read_csv(r'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\merge\merge_df\df_merge_month_operation.csv')
    
    df_goal_merge_day = calculate_cumplice_goals(df_result=df_result_day, df_goals=df_goal_day, name_file='result_day.xls')
    #df_goal_merge_month = calculate_cumplice_goals(df_result=df_result_month, df_goals=df_goal, name_file='result_day.xls')
    #df_goal_merge_month_operation = calculate_cumplice_goals(df_result=df_result_day, df_goals=df_goal, name_file='result_day.xls')
    
    
    print_test(df_goal_merge_day)

    