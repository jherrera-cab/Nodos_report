from data.conect.conect_db import connectSQL
from data.download.query_db import read_SQL
from data.config.config_query import var_date
from data.procesed.save_df_query import check_creation_dates_in_folder
from data.procesed.gestiones.gestiones import manipulation_gestion
from data.procesed.promises.promises import manipulation_promises
from data.procesed.read_df_back import read_df_back
from data.procesed.merge.merge import merge_df_summary
from data.procesed.master_aux.master_aux import calculate_aux
from print_test import print_test
from pathlib import Path

                                                         
entidades   =       ['NATURA2', 'MIBANCO', 'BANCOSANTANDER', 'NATURGY']
tipe_report =       ['diario','mensual']
entidad     =       entidades[0]
month_report=       10
date_variables      =   var_date(tipe_report[0], month_report=month_report)

result, faltantes      =       check_creation_dates_in_folder(date_variables['path_df_query'], 
                                                   date_variables['path_gestiones'],
                                                   entidad, 
                                                   date_variables['date_init_30'])


if result == 0 or len(faltantes) > 0:
    engine, connection  =   connectSQL()
    date_init_month     =   date_variables['date_init_month']
    read_SQL(cartera=entidad, date_variables=date_variables, month_report=month_report, faltantes=faltantes)
    dic_dfs =   read_df_back()
    manipulation_gestion(df=dic_dfs[f'df_gestion_month-{entidad}'], tipe_report=tipe_report[0], month_report=10, day_report=date_variables['day_report'])
    manipulation_promises(df_promises=dic_dfs[f'df_promises-{entidad}'], tipe_report=tipe_report[0], month_report=10, day_report=date_variables['day_report'])
    print('\n---------------------------------------------------')
    print('Se realizo la carga desde el Servidor')
    print('---------------------------------------------------\n')
else:
    dic_dfs =   read_df_back()
    print('\n---------------------------------------------------')
    print('Se realizo la carga desde el back de los DataFrame')
    print('---------------------------------------------------\n')
    

merge_df_summary()
calculate_aux(df_master_aux=dic_dfs[f'df_master_aux-{entidad}'], month_report=month_report)
