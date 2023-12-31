from docxtpl import  DocxTemplate
import subprocess

from print_test import print_test

    
def render_report(date_variables=None, 
                  entidad=None,
                  var_list=None,
                  summarys_acw=None,
                  list_df_goals=None
                  ):
    
    docx_tpl        =   DocxTemplate(date_variables['path_doc_input'])



    
    context={
        'day_report'                    :   date_variables['day_report'],
        'CARTERA'                       :   entidad,
        'FECHA_ACTUALIZACION'           :   date_variables['to_day'],
        'Coordinador'                   :   var_list['coordinador'],
        #Resumen encabezado
        'Fecha_presentacion'            :   date_variables['to_day'],
        'Inicio_reporte'                :   date_variables['date_init_30'],
        'Fin_reporte'                   :   date_variables['date_finish_30'],
        'Meta_gestiones_mes'            :   var_list['Meta_gestiones_mes'][0],
        'Meta_contacto_mes'             :   var_list['Meta_contacto_mes'][0],
        'Meta_promesas_mes'             :   var_list['Meta_promesas_mes'][0],
        'Meta_gestiones_dia'            :   var_list['Meta_gestiones_dia'][0],
        'Meta_contacto_dia'             :   var_list['Meta_contacto_dia'][0],
        'Meta_promesas_dia'             :   var_list['Meta_promesas_dia'][0],
        'Meta_gestiones_hora'           :   var_list['Meta_gestiones_hora'][0],
        'Meta_contacto_hora'            :   var_list['Meta_contacto_hora'][0],
        'Meta_promesas_hora'            :   var_list['Meta_promesas_hora'][0],
        'Total_horas'                   :   var_list['Total_horas'][0],
        #fin resumen
        'table_acw_week_0'              :   summarys_acw['summary_acw_weeks_1'],
        'table_acw_week_1'              :   summarys_acw['summary_acw_weeks_2'],
        'table_acw_week_2'              :   summarys_acw['summary_acw_weeks_3'],
        'table_acw_week_3'              :   summarys_acw['summary_acw_weeks_4'],
        'table_acw_week_4'              :   summarys_acw['summary_acw_weeks_5'],
        'table_month'                   :   list_df_goals['df_result_month_operation_play'],
        'table_day'                     :   list_df_goals['df_merge_aux_result_day'],
        #Comparativa detalle historico por asesor
        'table_history'                 :   list_df_goals['df_merge_aux_result_month'],
        #Comparativa operacion historica
        'table_history_operation'       :   list_df_goals['df_merge_aux_result_month_operation'],
        #Graficas
        #'grafic_radar_contact_operation':   grafic_contact_operation,
        #'grafic_distribution_contact_agent_positive':grafic_distribution_contact_agent_positive,
        #'grafic_distribution_contact_agent_negative':grafic_distribution_contact_agent_negative
    }
    
    #Renderizar la plantilla
    docx_tpl.render(context)
    
    #Exportar doc
    path_report = date_variables['path_doc_output'] 
    docx_tpl.save(path_report)
    
    read_report(path_report)
    
def read_report(ruta_archivo):
    try:
        subprocess.Popen(['start', '', ruta_archivo], shell=True)
    except FileNotFoundError:
        print("El archivo no existe.")
    except Exception as e:
        print(f"Error al abrir el archivo: {e}")