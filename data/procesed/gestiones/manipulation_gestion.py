from print_test import print_test
from datetime import timedelta
import pandas as pd
from data.procesed.save_df_query import save_query

def manipulation_gestion_table(df=None, type=None, name=None):  
    
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
        
        return df_result
    
    
        
    if type == 1:
        df_result = group_table_gestor(df=df)
        df_result = calculate_aht_acw(df_result)
        
        #Cambio de nombre para el encabezado de las nuevas filas
        df_result.loc[len(df_result) - 2, ['NOMBRE','COORDINADORA'] ] = ['Suma','-']
        df_result.loc[len(df_result) - 1, ['NOMBRE','COORDINADORA'] ] = ['Promedio','-']
  
        save_query(df=df_result, name=name, folder='procesed\gestiones\df_gestiones')
        
    else:
        df_result=group_table_operation(df)
        df_result = calculate_aht_acw(df_result)

        #Cambio de nombre para el encabezado de las nuevas filas
        df_result.loc[len(df_result) - 2, ['COORDINADORA'] ] = ['-']
        df_result.loc[len(df_result) - 1, ['COORDINADORA'] ] = ['-']
       
        save_query(df=df_result, name=name, folder='procesed\gestiones\df_gestiones')
    

    
    