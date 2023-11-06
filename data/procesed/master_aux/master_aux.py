from data.procesed.save_df_query import save_query
import pandas as pd
from print_test import print_test



def calculate_df_aux(df_aux_result=None, type='day', entidad=None):
    
    df_aux_result['hours_logueo']=round(df_aux_result['time']/60, 2)  
    aux_summary = pd.pivot_table(df_aux_result, values='time', index=['COORDINADORA', 'NOMBRE'], columns='AUX', fill_value=0).reset_index()
    if type=='day':
        avail_table=df_aux_result[df_aux_result['AUX'] == 'Avail']
        aux_summary['start_logueo'] = avail_table['start_logueo'].values
        aux_summary['end_logueo'] = avail_table['end_logueo'].values
        aux_summary=calculate_totals_columns(aux_summary=aux_summary,  entidad=entidad)
        order_columns=['COORDINADORA','NOMBRE', 'BackOffice', 'Baño', 'Break', 'Almuerzo', 'start_logueo', 'end_logueo', 'horas_logueo', 'aux_imporductivo', 'occupancy']
        aux_summary=aux_summary[order_columns]
        return aux_summary
    else:
        aux_summary_month=calculate_totals_columns(aux_summary=aux_summary,  entidad=entidad)
        order_columns=['COORDINADORA','NOMBRE', 'BackOffice', 'Baño', 'Break', 'Almuerzo', 'horas_logueo', 'aux_imporductivo', 'occupancy']
        aux_summary=aux_summary[order_columns]        
        return aux_summary_month



def calculate_totals_columns(aux_summary=None, entidad=None):
    
    def validate_column_aux(aux_summary=None):
        columns_aux=('COORDINADORA', 'NOMBRE','Avail', 'start_logueo', 'end_logueo','BackOffice', 'Baño', 'Break', 'Almuerzo')
        columns_summary = aux_summary.columns
        
        indice=[]
        for i, index in enumerate(columns_aux):
            if index not in columns_summary:
                aux_summary[index]=0
        return aux_summary
    
    df_goal_hour=pd.read_csv(fr'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\df\df_goals-{entidad}.csv')
    aux_summary=validate_column_aux(aux_summary=aux_summary)
    
    aux_summary['horas_logueo']=round(aux_summary['Avail']/60,2)
    aux_summary['aux_imporductivo']=aux_summary['BackOffice'] + aux_summary['Baño'] + aux_summary['Break']
    aux_summary['occupancy']=round((aux_summary['Avail'] - aux_summary['aux_imporductivo']) / aux_summary['Avail'], 2) * 100
       
    aux_summary['objetive_gestion']=round(aux_summary['horas_logueo'] * df_goal_hour['meta_gestion'][0],0)
    aux_summary['objetive_contact']=round(aux_summary['horas_logueo'] * df_goal_hour['meta_contacto'][0],0)
    aux_summary['objetive_promises']=round(aux_summary['horas_logueo'] * df_goal_hour['meta_promesas'][0],0)
    
    return aux_summary

def groupby_df(df_aux=None, type=None):
    if type == 'day':
        df_aux_result=df_aux.groupby(['DATE','NUM_WEEK', 'COORDINADORA', 'NOMBRE', 'AUX']).agg(
            start_logueo = ('HOUR', 'first'),
            end_logueo = ('HOUR', 'last'),
            time = ('TIME', 'sum')
        ).reset_index()
        return df_aux_result
    elif type == 'week':
        df_aux_result=df_aux.groupby(['NUM_WEEK', 'COORDINADORA', 'NOMBRE', 'AUX']).agg(
            start_logueo = ('HOUR', 'first'),
            end_logueo = ('HOUR', 'last'),
            time = ('TIME', 'sum')
        ).reset_index()
        return df_aux_result     
    else:
        df_aux_result=df_aux.groupby(['COORDINADORA', 'NOMBRE', 'AUX']).agg(
            start_logueo = ('HOUR', 'first'),
            end_logueo = ('HOUR', 'last'),
            time = ('TIME', 'sum')
        ).reset_index()
        return df_aux_result              

def calculate_aux(df_master_aux=None, entidad=None, month_report=None, day_report=None):

    df_master_aux_day=df_master_aux[df_master_aux['DATE'] == day_report]
    df_aux_result_day=groupby_df(df_aux=df_master_aux_day, type='day')

    df_aux_result_day=calculate_df_aux(df_aux_result=df_aux_result_day, type='day', entidad=entidad)
    save_query(df=df_aux_result_day, name='df_aux_result_day', folder=r'procesed\master_aux\master_aux_df')

    df_aux_result_month=groupby_df(df_aux=df_master_aux, type='month')
    df_aux_result_month=calculate_df_aux(df_aux_result=df_aux_result_month, type='month', entidad=entidad)
    save_query(df=df_aux_result_month, name='df_aux_result_month', folder=r'procesed\master_aux\master_aux_df')   
    
    df_aux_result_weeks=groupby_df(df_aux=df_master_aux, type='week')
    contador=1
    for week in range(df_master_aux['NUM_WEEK'].min(), df_master_aux['NUM_WEEK'].max(), 1):
        df_aux_result_week = df_aux_result_weeks[df_aux_result_weeks['NUM_WEEK'] == week]
        df_aux_result_week = calculate_df_aux(df_aux_result=df_aux_result_week, type='week', entidad=entidad)
        save_query(df=df_aux_result_week, name=f'df_aux_week_{contador}', folder=r'procesed\master_aux\master_aux_df')
        contador += 1
    
        
  

    
    