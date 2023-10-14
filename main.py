from data.conect.conect_db import connectSQL
from data.download.query_db import read_SQL
from data.config.config_query import var_date
from data.procesed.save_df_query import check_creation_dates_in_folder

result      =       check_creation_dates_in_folder(r'C:\Users\jherrera.FINANCREDITOS\OneDrive - Financreditos S.A.S\NodosLab\Nodos_Report\data\procesed\df')
                                                         
entidades   =       ['NATURA2', 'BANCOFINANDINA', 'MIBANCO', 'BANCOSANTANDER', 'NATURGY']
tipe_report =       ['diario','mensual']
#result=1

if result == 1:
    date_variables      =   var_date(tipe_report[0], month_report=9)
    engine, connection  =   connectSQL()
    date_init_month     =   date_variables['date_init_month']
    dfs = read_SQL(cartera=entidades[0], date_variables=date_variables)
else:
    print('No es necesario consultar BD')