from docxtpl import  DocxTemplate
import subprocess

def render_report(date_variables=None):
    
    docx_tpl        =   DocxTemplate(date_variables['path_doc_input'])
     
    context={
        'CARTERA'                       :   var_path[0],
        'FECHA_ACTUALIZACION'           :   var_path[1],
        'Coordinador'                   :   var_path[2],
        #Resumen encabezado
        'Fecha_presentacion'            :   var_list['Fecha presentación:'],
        'Inicio_reporte'                :   var_list['Fecha inicio reporte:'],
        'Fin_reporte'                   :   var_list['Fecha fin reporte:'],
        'Meta_gestiones_mes'            :   var_list['Meta gestiones mes:'],
        'Meta_contacto_mes'             :   var_list['Meta contacto mes:'],
        'Meta_promesas_mes'             :   var_list['Meta promesas mes:'],
        'Meta_gestiones_dia'            :   var_list['Meta gestiones dia:'],
        'Meta_contacto_dia'             :   var_list['Meta contacto dia:'],
        'Meta_promesas_dia'             :   var_list['Meta promesas dia:'],
        'Meta_gestiones_hora'           :   var_list['Meta gestiones hora:'],
        'Meta_contacto_hora'            :   var_list['Meta contacto hora:'],
        'Meta_promesas_hora'            :   var_list['Meta promesas hora:'],
        'Total_horas'                   :   var_list['Total horas'],
        #fin resumen
        'table_acw_week_0'              :   acw[0],
        'table_acw_week_1'              :   acw[1],
        'table_acw_week_2'              :   acw[2],
        'table_acw_week_3'              :   acw[3],
        'table_month'                   :   df_month_goal,
        'table_day'                     :   df_merge_day,
        #Comparativa detalle historico por asesor
        'table_history'                 :   df_merge_month,
        #Comparativa operacion historica
        'table_history_operation'       :   df_summary_operation,
        #Graficas
        'grafic_radar_contact_operation':   grafic_contact_operation,
        'grafic_distribution_contact_agent_positive':grafic_distribution_contact_agent_positive,
        'grafic_distribution_contact_agent_negative':grafic_distribution_contact_agent_negative
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