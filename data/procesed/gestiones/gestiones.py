from print_test import print_test
import pandas as pd
from data.procesed.save_df_query import save_query
from datetime import timedelta
import os
from pathlib import Path
from reports.convert_list.convert import df_to_list

def manipulation_gestion(df=None, tipe_report=None, month_report=None, day_report=None):
    
    df=df.sort_values(by='FECHA', ascending=False)
    df['FECHA']     =   pd.to_datetime(df['FECHA'], errors='coerce')
    df=df.sort_values(by= 'FECHA', ascending=False)

    gestion_day     =   df[df['FECHA'] == day_report]
    
    if len(gestion_day) <=1:
        print(  f'-----------------------------\n' 
                f'No se genera reporte para el dia {day_report}, ya que no se tiene registro de gestiones.\n'
                f'-----------------------------')
    else:
        manipulation_gestion_table(df=gestion_day, type=1, name='summary_gestion_day', month_report=month_report)
        manipulation_gestion_table(df=df, type=1, name='summary_gestion_month', month_report=month_report)
        manipulation_gestion_table(df=df, type=2, name='summary_gestion_operation_month', month_report=month_report)
        manipulation_gestion_table(df=df, type=3, name='summary_acw_weeks', month_report=month_report)
    


def manipulation_gestion_table(df=None, type=None, name=None, month_report=None):  
    
        
    
    def custom_count(x, condition):
        return (x == condition).sum()

    def group_table_gestor(df=None):
        df_result = df.groupby(['MES', 'COORDINADORA', 'NOMBRE']).agg(
            llave           =       ('llave', 'first'),
            count           =       ('ACCOUNT_NUMBER', 'nunique'),
            util_positive   =       ('TIPO_CONTACTO', lambda x: custom_count(x, 'UTIL POSITIVO')),
            util_negative   =       ('TIPO_CONTACTO', lambda x: custom_count(x, 'UTIL NEGATIVO')),
            no_contact      =       ('TIPO_CONTACTO', lambda x: custom_count(x, 'NO CONTACTO')),
            acw             =       ('ACW', 'sum'),
            aht             =       ('AHT', 'sum')
        ).reset_index()
        
        return df_result
    
    def calculate_aht_acw(df):
        df_result=df
        #Creación de nuevas columnas convirtiendo ACW y AHT en formato de horas para facilitar la lectura, se agrega la columna tasa de contacto
        df_result['tiempo_acw'] =       (df_result['acw']/3600).apply(lambda x: timedelta(hours=x))
        df_result['tiempo_acw'] =       df_result['tiempo_acw'].apply(lambda x: "{:02}:{:02}".format(x.seconds // 3600, (x.seconds % 3600) // 60))
        df_result['tiempo_aht'] =       (df_result['aht']/3600).apply(lambda x: timedelta(hours=x))
        df_result['tiempo_aht'] =       df_result['tiempo_aht'].apply(lambda x: "{:02}:{:02}".format(x.seconds // 3600, (x.seconds % 3600) // 60))
        df_result['tasa_contacto']=     round(df_result['util_positive'] / df_result['count'], 2) * 100
        df_result['tasa_contacto']=     df_result['tasa_contacto'].round(2)
        
        #Creación de nuevas filas con la suma y resta de todas las columnas numericas para comparar resultados, a columna promedio se aproximo a 2 decimales
        df_result_sum = df_result.sum(numeric_only=True)
        df_result_mean = df_result.mean(numeric_only=True)
        df_result_mean = round(df_result_mean, 2)

        #Cambio de orden el df con la suma de las columnas, pasando de columnas a filas
        df_sum=pd.DataFrame([df_result_sum], columns=df_result.columns)
        df_mean=pd.DataFrame([df_result_mean], columns=df_result.columns)
        
        #Union del DF agregado con las filas del nuevo calculo
        df_result=pd.concat([df_result, df_sum], ignore_index=True)
        df_result=pd.concat([df_result, df_mean], ignore_index=True)
        
        #Convertir suma de ACW y AHT en horas
        horas_acw=int(df_result['acw'].sum() /  3600)
        decimal_acw=int((df_result['acw'].sum() /  3600) - horas_acw) * 60
        horas_aht=int(df_result['aht'].sum() /  3600)
        decimal_aht=(df_result['aht'].sum() /  3600) - horas_aht
        #Cambiar formato del tiempo del ACW y AHT en HH:MM       
        df_result.at[len(df_result) - 1, 'tiempo_acw' ] = f"{horas_acw:02d}:{decimal_acw * 60:02.0f}"
        df_result.at[len(df_result) - 1, 'tiempo_aht' ] = f"{horas_aht:02d}:{decimal_aht * 60:02.0f}"
        
        return df_result
    
    def group_table_operation(df=None):
        df_result = df.groupby(['MES', 'COORDINADORA']).agg(
            count           =       ('ACCOUNT_NUMBER', 'nunique'),
            util_positive   =       ('TIPO_CONTACTO', lambda x: custom_count(x, 'UTIL POSITIVO')),
            util_negative   =       ('TIPO_CONTACTO', lambda x: custom_count(x, 'UTIL NEGATIVO')),
            no_contact      =       ('TIPO_CONTACTO', lambda x: custom_count(x, 'NO CONTACTO')),
            acw             =       ('ACW', 'sum'),
            aht             =       ('AHT', 'sum')
        ).reset_index()
        
        df_result['llave']=df_result['COORDINADORA'] + df_result['MES'].astype(str)
        return df_result
        
    def group_table_acw_week(df=None):
        
        df_result = df.groupby(['SEMANA','DIA_SEMANA', 'COORDINADORA','NOMBRE']).agg(
            count           =       ('ACCOUNT_NUMBER', 'nunique'),
            util_positive   =       ('TIPO_CONTACTO', lambda x: custom_count(x, 'UTIL POSITIVO')),
            util_negative   =       ('TIPO_CONTACTO', lambda x: custom_count(x, 'UTIL NEGATIVO')),
            no_contact      =       ('TIPO_CONTACTO', lambda x: custom_count(x, 'NO CONTACTO')),
            acw             =       ('ACW', 'sum'),
            aht             =       ('AHT', 'sum')
        ).reset_index()
        
        
        return df_result        
    
    folder='procesed\gestiones\df_gestiones'
        
    if type == 1:
        
        df_result = group_table_gestor(df=df)
        df_result = calculate_aht_acw(df_result)
        
        #Cambio de nombre para el encabezado de las nuevas filas
        df_result.loc[len(df_result) - 2, ['NOMBRE','COORDINADORA', 'llave', 'MES'] ] = ['Suma','-', 'Suma10','']
        df_result.loc[len(df_result) - 1, ['NOMBRE','COORDINADORA', 'llave', 'MES'] ] = ['Promedio','-', 'Promedio10','']
        
        save_query(df=df_result, name=name, folder=folder)
        
    elif type == 2:
        df_result=group_table_operation(df)
        df_result = calculate_aht_acw(df_result)

        #Cambio de nombre para el encabezado de las nuevas filas
        df_result.loc[len(df_result) - 2, ['COORDINADORA', 'MES'] ] = ['Suma','']
        df_result.loc[len(df_result) - 1, ['COORDINADORA', 'MES'] ] = ['Promedio','']
       
        save_query(df=df_result, name=name, folder=folder)
        
    else:
        
        df_weeks = df[df['MES'] == month_report]
        
        contador = 1
        for week in range(df_weeks['SEMANA'].min(), df_weeks['SEMANA'].max() + 1):
        
            df_weeks_filter=df_weeks[df_weeks['SEMANA'] == week]
            df_result=group_table_acw_week(df_weeks_filter)  
            
            df_result = calculate_aht_acw(df_result)
            
            #df_result=df_result[['SEMANA','DIA_SEMANA', 'COORDINADORA','NOMBRE','tiempo_acw', 'tiempo_aht']]
            df_result.loc[len(df_result) - 2, ['COORDINADORA', 'MES'] ] = ['Suma','']
            df_result.loc[len(df_result) - 1, ['COORDINADORA', 'MES'] ] = ['Promedio','']  
            days_week = {
               0:'lunes',
               1: 'martes',
               2: 'miercoles',
               3: 'jueves',
               4: 'viernes',
               5: 'sabado'
            }
                       
            df_result['DIA_SEMANA'] = df_result['DIA_SEMANA'].map(days_week)
            df_result=df_result.iloc[:-2]

            df_result['llave'] = df_result['NOMBRE'] + "_" + df_result['DIA_SEMANA']
            df_result = df_result.pivot(index=['COORDINADORA', 'NOMBRE'], columns=['DIA_SEMANA'], values=['tiempo_acw', 'acw' , 'tiempo_aht', 'aht'])

            columns=df_result.columns
            new_columns=[]
            for column in columns:
                new_column = '_'.join(column).replace('-','_')
                new_columns.append(new_column)
            
            df_result.columns=new_columns
            df_result.reset_index(inplace=True)

            df_result.fillna(0, inplace=True)
            columns_sum=['acw_jueves', 'acw_lunes', 'acw_martes', 'acw_miercoles', 'acw_sabado', 'acw_viernes']
            df_result['total_acw']= 0
            
            for column in columns_sum:
                if column in df_result.columns:
                    df_result['total_acw'] += df_result[column]
            df_result['tiempo_total_acw'] =       (df_result['total_acw']/3600).apply(lambda x: timedelta(hours=x))
            df_result['tiempo_total_acw'] =       df_result['tiempo_total_acw'].apply(lambda x: "{:02}:{:02}".format(x.seconds // 3600, (x.seconds % 3600) // 60))
      
            name_file=name + '_' + str(contador)          
            save_query(df=df_result, name=name_file, folder=folder)
            contador += 1
            


        
