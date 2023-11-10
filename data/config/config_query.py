from datetime import datetime, timedelta
from print_test import print_test
from pathlib import Path
import os

def var_date(tipe_report=None, month_report=None, entidad=None):
    
 
    
    date_init_30    =   datetime.today()
    date_finish_30  =   date_init_30 - timedelta(days=30)
    
    if tipe_report == 'mensual':
        date_init_month =   date_init_30.replace(day=1, month= month_report)
        date_finish_month = datetime.today() - timedelta(days=30) 
    else:
        month_report    =   date_init_30.month
        date_init_month =   date_init_30.replace(day=1, month= month_report)
        date_finish_month = datetime.today() - timedelta(days=1)
        
    date_init_month_n1 = date_init_month.replace(month=month_report-1)
    date_finish_month_n1 = date_finish_month.replace(month=month_report-1)
    
    date_init_month_n2 = date_init_month.replace(month=month_report-2)
    date_finish_month_n2 = date_finish_month.replace(month=month_report-2, day=30)
    
    date_init_month_n3 = date_init_month.replace(month=month_report-3)
    date_finish_month_n3 = date_finish_month.replace(month=month_report-3)
    
    num_day_week    =   date_init_30.weekday()
    init_week       =   date_init_30 - timedelta(days   =   num_day_week)
    finish_week     =   date_finish_30 + timedelta(days=(4-num_day_week))
    days_full       =   0
    days_medio      =   0
    weeks           =   0
    
    day_report      =   datetime.today() 
    to_day          =   datetime.today().strftime('%Y-%m-%d')
            
    if day_report.weekday() == 0:
        day_report      =   day_report      -   timedelta(days=2)
        day_report      =   day_report.strftime('%Y-%m-%d')
        time_day        =   4
    else:
        day_report      =   day_report      -   timedelta(days=1)#1
        day_report      =   day_report.strftime('%Y-%m-%d')
        time_day        =   9
        

    current_day = date_init_month
    days_full=1
    while current_day <= date_finish_month - timedelta(days=1):
        if current_day.weekday() <= 4:  # 0 a 4 representan días laborables (lunes a viernes)
            days_full += 1
            
        elif current_day.weekday() == 5:  # 5 representa el sábado
            days_medio += 1
        if current_day.weekday() == 6:
            weeks += 1
        # Incrementa la fecha actual en un día
        current_day += timedelta(days=1)
        
    hour_total=(days_full * 9) + (days_medio * 2)
    
    num_week = datetime.today().isocalendar()[1]
    
    raiz=Path(__file__).resolve().parents[1]

    path_gestiones=os.path.join(raiz,'procesed\gestiones\df_gestiones')
    path_df_query=os.path.join(raiz,'procesed\df')
    
    path_doc_input  =   fr"Z:\1. Coordinadores\2. Jonathan Herrera\Estrategia\Reportes seguimiento\Plantilla reporte.docx"
    folder_report   =   fr'Z:\1. Coordinadores\2. Jonathan Herrera\Estrategia\Reportes seguimiento'
    
    name_doc        =   entidad + ' ' + day_report + '.docx'
    name_xls        =   'Seguimientos ' + entidad + ' ' + day_report + '.xlsx'
    path_doc_output =   os.path.join(folder_report, entidad, name_doc) 
    path_xls_output =   os.path.join(folder_report, entidad, name_xls) 
    
        
    var_config={
        'day_report'        :   day_report,
        'month_report'      :   month_report,   
        'num_day_week'      :   num_day_week,
        'weeks'             :   weeks,   
        'num_week'          :   num_week,
        'date_init_30'      :   date_init_30,           
        'date_finish_30'    :   date_finish_30,         
        'date_init_month'   :   date_init_month,        
        'date_finish_month' :   date_finish_month,      
        'date_init_month_n1':   date_init_month_n1,     
        'date_finish_month_n1': date_finish_month_n1,   
        'date_init_month_n2':   date_init_month_n2,     
        'date_finish_month_n2': date_finish_month_n2,   
        'date_init_month_n3':   date_init_month_n3,    
        'date_finish_month_n3': date_finish_month_n3,  
        'days_full'         :   days_full,              
        'days_medio'        :   days_medio,            
        'hour_total'        :   hour_total,            
        'path_df_query'     :   path_df_query,
        'path_gestiones'    :   path_gestiones,
        'path_doc_input'    :   path_doc_input,
        'path_doc_output'   :   path_doc_output,
        'path_xls_output'   :   path_xls_output,
        'time_day'          :   time_day,
        'to_day'            :   to_day
    }
    

    
    return var_config
    
    
