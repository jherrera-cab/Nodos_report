from print_test import print_test
from data.procesed.gestiones.manipulation_gestion import manipulation_gestion_table
import pandas as pd

def manipulation_gestion(df=None, tipe_report=None, month_report=None, day_report=None):
    df['FECHA']     =   pd.to_datetime(df['FECHA'], errors='coerce')
    df=df.sort_values(by= 'FECHA', ascending=False)
    gestion_day     =   df[df['FECHA'] == day_report]

    if len(gestion_day) <=1:
        print(  f'-----------------------------\n' 
                f'No se genera reporte para el dia {day_report}, ya que no se tiene registro de gestiones.\n'
                f'-----------------------------')
    else:
        manipulation_gestion_table(df=gestion_day, type=1, name='summary_gestion_day')
        manipulation_gestion_table(df=df, type=1, name='summary_gestion_month')
        manipulation_gestion_table(df=df, type=2, name='summary_gestion_operation_month')
