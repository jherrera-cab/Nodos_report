from data.procesed.save_df_query import save_query
import pandas as pd
from print_test import print_test

def calculate_df_aux(df_aux_result=None, name_file=None):
    print_test(df_aux_result)        
    df_aux_result['hours_logueo']=round(df_aux_result['time']/60, 2)
    
    avail_table=df_aux_result[df_aux_result['AUX'] == 'Avail']
    
    aux_summary = pd.pivot_table(df_aux_result, values='time', index='NOMBRE', columns='AUX', fill_value=0).reset_index()

    aux_summary['start_logueo'] = avail_table['start_logueo'].values
    aux_summary['end_logueo'] = avail_table['end_logueo'].values
    aux_summary['horas_logueo']=round(aux_summary['Avail']/60,2)
    aux_summary['aux_imporductivo']=aux_summary['BackOffice'] + aux_summary['Baño'] + aux_summary['Break']
    aux_summary['occupancy']=round((aux_summary['Avail'] - aux_summary['aux_imporductivo']) / aux_summary['Avail'], 2) * 100
    
    df_goal_hour=pd.read_csv(r'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\df\df_goals-NATURA2.csv')
    
    aux_summary['objetive_gestion']=round(aux_summary['horas_logueo'] * df_goal_hour['meta_gestion'][0],0)
    aux_summary['objetive_contact']=round(aux_summary['horas_logueo'] * df_goal_hour['meta_contacto'][0],0)
    aux_summary['objetive_promises']=round(aux_summary['horas_logueo'] * df_goal_hour['meta_promesas'][0],0)
    
    save_query(df=aux_summary, name=name_file, folder=r'procesed\master_aux\master_aux_df')
    return aux_summary

def calculate_aux(df_master_aux=None, month_report=None, day_report=None):

    
    df_master_aux=df_master_aux[df_master_aux['MONTH'] == month_report]

    df_aux_result=df_master_aux.groupby(['DATE','NUM_WEEK', 'COORDINADORA', 'NOMBRE', 'AUX']).agg(
        start_logueo = ('HOUR', 'first'),
        end_logueo = ('HOUR', 'last'),
        time = ('TIME', 'sum')
    ).reset_index()

    order_aux=['Avail', 'Almuerzo', 'Baño', 'Break', 'BackOffice']
    df_aux_result['AUX_Order'] = df_aux_result['AUX'].apply(lambda x: order_aux.index(x))
    
    df_aux_result=df_aux_result.sort_values(by=['DATE', 'NOMBRE', 'AUX_Order'], ascending=[True, True, True])
    
    
    df_aux_result_day=df_aux_result[df_aux_result['DATE'] == day_report]
    
    calculate_df_aux(df_aux_result=df_aux_result_day, name_file='df_result_loguin_day')

    week_init=df_aux_result['NUM_WEEK'].iloc[0]
    week_end=df_aux_result['NUM_WEEK'].iloc[-1]
    df_aux_result_week_1=df_aux_result[df_aux_result['NUM_WEEK']==week_init]
    #calculate_df_aux(df_aux_result=df_aux_result_week_1, name_file='df_aux_result_month')
    
    
    
    
    