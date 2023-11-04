import pandas as pd
from print_test import print_test
from data.procesed.save_df_query import save_query

def calculate_acw(df_gestiones=None, day_report=None):
    df_gestiones=df_gestiones[df_gestiones['FECHA'] == day_report]
    df_summary_acw=df_gestiones.groupby(['COORDINADORA', 'NOMBRE', 'TIPO_CONTACTO']).agg(
        AHT = ('AHT', 'sum'),
        ACW = ('ACW', 'sum')
        ).reset_index()
    
    df_summary_acw_pivot = pd.pivot_table(df_summary_acw, values=['AHT', 'ACW'], index=['COORDINADORA', 'NOMBRE'], columns='TIPO_CONTACTO', fill_value=0).reset_index()

    column_names = [col[0] if col[1] == '' else f"{col[0]} - {col[1]}" for col in df_summary_acw_pivot.columns]
    df_summary_acw_pivot.columns = column_names
    df_summary_acw_pivot['Total_ACW'] = df_summary_acw_pivot['ACW - NO CONTACTO'] + df_summary_acw_pivot['ACW - UTIL NEGATIVO'] + df_summary_acw_pivot['ACW - UTIL POSITIVO']
    df_summary_acw_pivot['Total_AHT'] = df_summary_acw_pivot['AHT - NO CONTACTO'] + df_summary_acw_pivot['AHT - UTIL NEGATIVO'] + df_summary_acw_pivot['AHT - UTIL POSITIVO']

    save_query(df=df_summary_acw_pivot, name='df_summary_acw_pivot', folder=r'procesed\acw\df_acw')
    
    