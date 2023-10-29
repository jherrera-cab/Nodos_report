import os
from datetime import datetime
from print_test import print_test
from pathlib import Path


def save_query(df=None, name=None, folder=None):
    raiz=Path(__file__).resolve().parents[1]
    path=raiz
    #r'C:\Users\jherrera.FINANCREDITOS\OneDrive - Financreditos S.A.S\NodosLab\Nodos_Report\data'
    
    file_path=os.path.join(path, folder, name + '.csv')
    df.to_csv(file_path, index=False)

def get_creation_date(file_path):
    date_creation_file=datetime.fromtimestamp(os.path.getctime(file_path))
    return date_creation_file


def check_creation_dates_in_folder(path_df_query, 
                                   path_gestiones, 
                                   entidad, 
                                   date):
    
    dfs=[
        f'df_gestion_month-{entidad}',
        f'df_goals-{entidad}',
        f'df_promises-{entidad}', 
        f'df_efect-{entidad}',
        f'df_seguimientos_promesas-{entidad}',
        f'df_master_aux-{entidad}'
        ]
    
    
    files = [f for f in os.listdir(path_df_query) if os.path.isfile(os.path.join(path_df_query, f))]

    date_creation=[]
    for file in files:
        path_file=os.path.join(path_df_query, file)
        date_file=get_creation_date(path_file).date()
        date_creation.append(date_file)
    creation_dates = [get_creation_date(os.path.join(path_df_query, f)).date() for f in files]
    date=date.today().date()


    
    dfs=os.listdir(path_df_query)
    
    for df in dfs:
        name_ext, ext = df.split('.')
        name, sufijo=name_ext.split('-')
        if sufijo != entidad:
            file=os.path.join(path_df_query, df)
            os.remove(file)
            
    dfs_esperados=(
        'df_goals',
        'df_gestion_month',
        'df_promises',
        'df_efect',
        'df_seguimientos_promesas',
        'df_master_aux'
    )       
    dic_file=[]
    for i in dfs:
        name_csv=i.split('-')
        dic_file.append(name_csv[0])
        

    indice=[]
    for i, index in enumerate(dfs_esperados):
        if index not in dic_file:   
            indice.append(i)
        
            
    # Comprueba si todas las fechas de creación son iguales
    q_file=len(os.listdir(path_df_query))
    if q_file >= 1:
        if date != creation_dates[0]:
            for filename in os.listdir(path_df_query):
                file=os.path.join(path_df_query, filename)
                os.remove(file)
                
                for file_gestion in os.listdir(path_gestiones):
                    file_gestion_name = os.path.join(path_gestiones, file_gestion)
                    os.remove(file_gestion_name)
            return 0, indice
        else:
            return 1, indice
    else:
        return 0, indice