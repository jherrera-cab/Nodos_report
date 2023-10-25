from print_test import print_test
from datetime import timedelta
import pandas as pd
from data.procesed.save_df_query import save_query


def manipulation_promises_table(df_promises=None, type=None, name=None):  
    
    def format_currency(value, currency_symbol):
        return  f"{currency_symbol}{value:,.2f}"
    
    def custom_count(x, condition):
        return (x == condition).sum()

    def group_table_gestor(df=None):
        # The code is performing a groupby operation on the DataFrame `df` using the columns 'MES',
        # 'COORDINADORA', and 'NOMBRE'. It then aggregates the data using the following functions:
        df_result = df.groupby(['MES', 'COORDINADORA', 'NOMBRE']).agg(
            llave           =       ('llave', 'first'),
            count_promises           =       ('ACCOUNT_NUMBER', 'nunique'),
            ticket          =       ('PROMISE_NEXT_AMOUNT', 'sum'),
            ticket_medio    =       ('PROMISE_NEXT_AMOUNT', 'mean'),
        ).reset_index()
        
        return df_result
    
    def group_table_operation(df=None):
        # The code is performing a groupby operation on the DataFrame `df` using the columns 'MES' and
        # 'COORDINADORA'. It then aggregates the data using various functions:
        df_result = df.groupby(['MES', 'COORDINADORA']).agg(
            count_promises           =       ('ACCOUNT_NUMBER', 'nunique'),
            ticket          =       ('PROMISE_NEXT_AMOUNT', 'sum'),
            ticket_medio    =       ('PROMISE_NEXT_AMOUNT', 'mean'),
        ).reset_index()
        df_result['llave']=df_result['COORDINADORA'] + df_result['MES'].astype(str)
        return df_result
    
    def row_sum_mean(df_promises):
        #Creación de nuevas filas con la suma y resta de todas las columnas numericas para comparar resultados, a columna promedio se aproximo a 2 decimales
        df_result_sum  = df_promises.sum(numeric_only=True)
        df_result_mean = df_promises.mean(numeric_only=True)
        df_result_mean = round(df_result_mean, 2)
       
        #Cambio de orden el df con la suma de las columnas, pasando de columnas a filas
        df_sum=pd.DataFrame([df_result_sum], columns=df_promises.columns)
        df_mean=pd.DataFrame([df_result_mean], columns=df_promises.columns)

        #Union del DF agregado con las filas del nuevo calculo
        df_result=pd.concat([df_promises, df_sum], ignore_index=True)
        df_result=pd.concat([df_result, df_mean], ignore_index=True)
                
        return df_result
    
    def manipulation_currency(df_result):
        # is applying the `format_currency` function to the 'ticket' column of the DataFrame
        # `df_result`. This function formats the values in the 'ticket' column as currency, with the
        # currency symbol '$'.
        df_result['ticket'] = df_result['ticket'].apply(lambda x: format_currency(x,'$'))
        df_result['ticket_medio'] = df_result['ticket_medio'].apply(lambda x: format_currency(x,'$'))         
        
        return df_result
    
    if type == 1:
        df_result=group_table_gestor(df_promises)
        df_result=row_sum_mean(df_result)
        df_result=manipulation_currency(df_result)
        
        #Cambio de nombre para el encabezado de las nuevas filas
        df_result.loc[len(df_result) - 2, ['COORDINADORA', 'NOMBRE', 'MES', 'llave'] ] = [0, 'Suma',0, 'Suma10']
        df_result.loc[len(df_result) - 1, ['COORDINADORA', 'NOMBRE', 'MES', 'llave'] ] = [0, 'Promedio',0, 'Promedio10']
        
        save_query(df=df_result, name=name, folder='procesed\promises\df_promises')       
        
    else:
        df_result=group_table_operation(df_promises)
        df_result=row_sum_mean(df_result)
        df_result=manipulation_currency(df_result)
        
        #Cambio de nombre para el encabezado de las nuevas filas
        df_result.loc[len(df_result) - 2, ['COORDINADORA', 'MES', 'llave'] ] = ['Suma',0, 'Suma10']
        df_result.loc[len(df_result) - 1, ['COORDINADORA', 'MES', 'llave'] ] = ['Promedio',0, 'Promedio10']
        
        save_query(df=df_result, name=name, folder='procesed\promises\df_promises')   
        
        