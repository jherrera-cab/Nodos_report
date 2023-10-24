from data.conect.conect_db import connectSQL
from data.download.query_db import read_SQL
from data.config.config_query import var_date
from data.procesed.save_df_query import check_creation_dates_in_folder
from data.procesed.gestiones.gestiones import manipulation_gestion
from data.procesed.promises.promises import manipulation_promises
from data.procesed.read_df_back import read_df_back
from print_test import print_test

                                                         
entidades   =       ['NATURA2', 'MIBANCO', 'BANCOSANTANDER', 'NATURGY']
tipe_report =       ['diario','mensual']
entidad     =       entidades[1]
date_variables      =   var_date(tipe_report[0], month_report=9)

result      =       check_creation_dates_in_folder(date_variables['path_df_query'], 
                                                   date_variables['path_gestiones'],
                                                   entidad, 
                                                   date_variables['date_init_30'])


if result == 0:
    engine, connection  =   connectSQL()
    date_init_month     =   date_variables['date_init_month']
    dic_dfs = read_SQL(cartera=entidad, date_variables=date_variables)
    print('\n---------------------------------------------------')
    print('Se realizo la carga desde el Servidor')
    print('---------------------------------------------------\n')
else:
    print('\n---------------------------------------------------')
    print('Se realizo la carga desde el back de los DataFrame')
    print('---------------------------------------------------\n')
    dic_dfs =   read_df_back()

manipulation_gestion(df=dic_dfs[f'df_gestion_month-{entidad}'], tipe_report=tipe_report[0], month_report=10, day_report=date_variables['day_report'])
manipulation_promises(df=dic_dfs[f'df_promises-{entidad}'], tipe_report=tipe_report[0], month_report=10, day_report=date_variables['day_report'])