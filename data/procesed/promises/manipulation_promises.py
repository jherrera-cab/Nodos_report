from print_test import print_test
from datetime import timedelta
import pandas as pd
from data.procesed.save_df_query import save_query

def manipulation_promises_table(df=None, type=None, name=None):  
    def custom_count(x, condition):
        return (x == condition).sum()

    def group_table_gestor(df=None):
        # The code is performing a groupby operation on the DataFrame `df` using the columns 'MES',
        # 'COORDINADORA', and 'NOMBRE'. It then aggregates the data using the following functions:
        df_result = df.groupby(['MES', 'COORDINADORA', 'NOMBRE']).agg(
            llave           =       ('llave', 'first'),
            count           =       ('ACCOUNT_NUMBER', 'nunique'),
            ticket          =       ('PROMISE_NEXT_AMOUNT', 'sum'),
            ticket_medio    =       ('PROMISE_NEXT_AMOUNT', 'mean'),
        ).reset_index()
        
        return df_result
    
    def group_table_operation(df=None):
        # The code is performing a groupby operation on the DataFrame `df` using the columns 'MES' and
        # 'COORDINADORA'. It then aggregates the data using various functions:
        df_result = df.groupby(['MES', 'COORDINADORA']).agg(
            count           =       ('ACCOUNT_NUMBER', 'nunique'),
            util_positive   =       ('TIPO_CONTACTO', lambda x: custom_count(x, 'UTIL POSITIVO')),
            util_negative   =       ('TIPO_CONTACTO', lambda x: custom_count(x, 'UTIL NEGATIVO')),
            no_contact      =       ('TIPO_CONTACTO', lambda x: custom_count(x, 'NO CONTACTO')),
            acw             =       ('ACW', 'sum'),
            aht             =       ('AHT', 'sum')
        ).reset_index()
        
        return df_result
    
    df_result=group_table_gestor(df)