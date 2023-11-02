import pandas as pd
from print_test import print_test
from data.procesed.save_df_query import save_query

def calculate_goal(entidad=None, num_day=None):
    
    
    df_goal=pd.read_csv(rf'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\df\df_goals-{entidad}.csv')
    df_aux_day=pd.read_csv(r'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\master_aux\master_aux_df\df_aux_result_day.csv')
    df_result_day=pd.read_csv(r'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\merge\merge_df\df_merge_day.csv')
    
    if num_day == 0:
        hour_expected = 4
    else:
        hour_expected = 9
    
    df_aux_day['objetive_gestion']=df_aux_day['horas_logueo']*df_goal.at[0, 'meta_gestion']
    df_aux_day['expected_gestion']=hour_expected *df_goal.at[0, 'meta_gestion']
    df_aux_day['objetive_gestion']=df_aux_day['objetive_gestion'].round(2)

    df_aux_day['objetive_contact']=df_aux_day['horas_logueo']*df_goal.at[0, 'meta_contacto']
    df_aux_day['expected_contact']=hour_expected *df_goal.at[0, 'meta_contacto']    
    df_aux_day['objetive_contact']=df_aux_day['objetive_contact'].round(2)
    
    df_aux_day['objetive_promises']=df_aux_day['horas_logueo']*df_goal.at[0, 'meta_promesas']
    df_aux_day['expected_promises']=hour_expected *df_goal.at[0, 'meta_promesas']   
    df_aux_day['objetive_promises']=df_aux_day['objetive_promises'].round(2)
    
    df_merge_aux_result= pd.merge(df_aux_day,df_result_day, on='NOMBRE', how='left')
    save_query(df=df_merge_aux_result, name='df_merge_aux_result', folder=r'procesed\goals\goals_df')
    print_test(df_merge_aux_result)

    