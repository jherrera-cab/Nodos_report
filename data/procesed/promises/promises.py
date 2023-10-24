
from print_test import print_test
from data.procesed.promises.manipulation_promises import manipulation_promises_table

def manipulation_promises(df_promises=None, tipe_report=None, month_report=None, day_report=None):
    promises_month  =   df_promises
    promises_day    =   df_promises[df_promises['FECHA'] == day_report]
    
    if len(promises_day) <= 1:
        print(  f'-----------------------------\n' 
                f'No se genera reporte para el dia {day_report}, ya que no se tiene registro de gestiones.\n'
                f'-----------------------------')
    else:
        manipulation_promises_table(df=None, type=None, name=None)