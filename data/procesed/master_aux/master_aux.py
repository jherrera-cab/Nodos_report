from print_test import print_test
import pandas as pd


def calculate_aux(df_master_aux=None, month_report=None):
    df_master_aux=df_master_aux[df_master_aux['MONTH'] == month_report]

    df_aux_result=df_master_aux.groupby(['DATE', 'NAME', 'AUX']).agg(
        start_logueo = ('HOUR', 'first'),
        end_logueo = ('HOUR', 'last'),
        time = ('TIME', 'sum')
    ).reset_index()
    
    order_aux=['Avail', 'Almuerzo', 'Baño', 'Break', 'BackOffice']
    df_aux_result['AUX_Order'] = df_aux_result['AUX'].apply(lambda x: order_aux.index(x))
    
    df_aux_result=df_aux_result.sort_values(by=['DATE', 'NAME', 'AUX_Order'], ascending=[True, True, True])
    df_aux_result=df_aux_result[df_aux_result['DATE'] == '2023-10-26']
    
    df_aux_result['hours_logueo']=round(df_aux_result['time']/60, 2)
    avail_table=df_aux_result[df_aux_result['AUX'] == 'Avail']

    aux_summary = pd.pivot_table(df_aux_result, values='time', index='NAME', columns='AUX', fill_value=0)
    aux_summary['start_logueo'] = avail_table['start_logueo'].values
    aux_summary['end_logueo'] = avail_table['end_logueo'].values
    
    print_test(aux_summary)


