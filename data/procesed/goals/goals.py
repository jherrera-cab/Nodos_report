import pandas as pd
from print_test import print_test
from data.procesed.save_df_query import save_query

def calculate_goal(entidad=None, num_day=None):
    def color_percentil(kpi):
        color={
            'top':'#a7c957',
            'ontarget':'#ffb627',
            'medium':'#e2711d',
            'low':'#d62828',
            }
        

        if kpi <= 50:
            return color['low']
        elif kpi <= 70:
            return color['medium']
        elif kpi <= 90:
            return color['ontarget']
        else:
            return color['top']

    
    def row_total_mean(df_result_suma=None):
        suma_row = df_result_suma.select_dtypes(include=['number']).sum()
        suma_row.name = 'Suma'
        suma_row=suma_row.round(2)
       
        mean_row = df_result_suma.select_dtypes(include=['number']).mean()
        mean_row.name = 'Promedio'  
        mean_row=mean_row.round(2)
        
        df_suma_promedio = pd.DataFrame([suma_row, mean_row], index=['Suma', 'Promedio'])

        df_result_suma=pd.concat([df_result_suma, df_suma_promedio])
        
        return df_result_suma

            
    def calculate_goal_df(df_aux=None, df_goal=None, df_result = None):
        
        hour_expected = 4 if num_day == 0 else 9
           
        weight_gestion              =   float(df_goal['peso_gestion'].iloc[0])
        weight_contact              =   float(df_goal['peso_contacto'].iloc[0])
        weight_promises             =   float(df_goal['peso_promesas'].iloc[0])
        
        df_aux['objetive_gestion']=df_aux['horas_logueo']*df_goal.at[0, 'meta_gestion']
        df_aux['expected_gestion']=hour_expected *df_goal.at[0, 'meta_gestion']
        df_aux['objetive_gestion']=df_aux['objetive_gestion'].round(2)

        df_aux['objetive_contact']=df_aux['horas_logueo']*df_goal.at[0, 'meta_contacto']
        df_aux['expected_contact']=hour_expected *df_goal.at[0, 'meta_contacto']    
        df_aux['objetive_contact']=df_aux['objetive_contact'].round(2)
        
        df_aux['objetive_promises']=df_aux['horas_logueo']*df_goal.at[0, 'meta_promesas']
        df_aux['expected_promises']=hour_expected *df_goal.at[0, 'meta_promesas']   
        df_aux['objetive_promises']=df_aux['objetive_promises'].round(2)
        
        df_merge_aux_result= pd.merge(df_aux_day,df_result, on='NOMBRE', how='left')
        
        df_merge_aux_result['compliance_gestion'] = df_merge_aux_result['count'] / df_merge_aux_result['objetive_gestion'] *100
        df_merge_aux_result['compliance_contact'] = df_merge_aux_result['util_positive'] / df_merge_aux_result['objetive_contact'] *100
        df_merge_aux_result['compliance_promises'] = df_merge_aux_result['count_promises'] / df_merge_aux_result['objetive_promises'] *100
        
        df_merge_aux_result['compliance_gestion'] =df_merge_aux_result['compliance_gestion'].round(2)
        df_merge_aux_result['compliance_contact'] =df_merge_aux_result['compliance_contact'].round(2)
        df_merge_aux_result['compliance_promises'] =df_merge_aux_result['compliance_promises'].round(2)
        
        df_merge_aux_result['compliance_expected_gestion'] = df_merge_aux_result['count'] / df_merge_aux_result['expected_gestion'] *100
        df_merge_aux_result['compliance_expected_contact'] = df_merge_aux_result['util_positive'] / df_merge_aux_result['expected_contact'] *100
        df_merge_aux_result['compliance_expected_promises'] = df_merge_aux_result['count_promises'] / df_merge_aux_result['expected_promises'] *100
        
        df_merge_aux_result['compliance_expected_gestion'] =df_merge_aux_result['compliance_expected_gestion'].round(2)
        df_merge_aux_result['compliance_expected_contact'] =df_merge_aux_result['compliance_expected_contact'].round(2)
        df_merge_aux_result['compliance_expected_promises'] =df_merge_aux_result['compliance_expected_promises'].round(2)
        
        df_merge_aux_result['color_gestion_percetil'] = df_merge_aux_result['compliance_gestion'].apply(lambda x: color_percentil(x))
        df_merge_aux_result['color_contact_percetil'] = df_merge_aux_result['compliance_contact'].apply(lambda x: color_percentil(x))
        df_merge_aux_result['color_promises_percetil'] = df_merge_aux_result['compliance_promises'].apply(lambda x: color_percentil(x))
        
        df_merge_aux_result['percentil'] = ((df_merge_aux_result['compliance_gestion'] * weight_gestion) + (df_merge_aux_result['compliance_contact'] * weight_contact) + (df_merge_aux_result['compliance_promises']  * weight_promises) ) / 100
        df_merge_aux_result['percentil'] = df_merge_aux_result['percentil'].round(2)
        df_merge_aux_result['color_percentil'] = df_merge_aux_result['percentil'].apply(lambda x: color_percentil(x))
        
        
        return df_merge_aux_result
    
    df_goal=pd.read_csv(rf'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\df\df_goals-{entidad}.csv')
    df_aux_day=pd.read_csv(r'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\master_aux\master_aux_df\df_aux_result_day.csv')
    df_aux_day=pd.read_csv(r'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\master_aux\master_aux_df\df_aux_result_month.csv')
    df_result_day=pd.read_csv(r'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\merge\merge_df\df_merge_day.csv')
    df_result_month=pd.read_csv(r'Z:\1. Coordinadores\2. Jonathan Herrera\Scripts\NodosLab\Nodos_Lab_Report\data\procesed\merge\merge_df\df_merge_month.csv')

    df_merge_aux_result_day=calculate_goal_df(df_aux=df_aux_day, df_goal= df_goal, df_result = df_result_day)
    df_merge_aux_result_month= calculate_goal_df(df_aux=df_aux_day, df_goal= df_goal, df_result = df_result_month)
    df_result_day=row_total_mean(df_merge_aux_result_day)
    df_result_month=row_total_mean(df_merge_aux_result_month)
    
    save_query(df=df_result_day, name='df_merge_aux_result_day', folder=r'procesed\goals\goals_df')
    save_query(df=df_result_month, name='df_merge_aux_result_month', folder=r'procesed\goals\goals_df')
    

    