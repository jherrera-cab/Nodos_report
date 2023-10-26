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
            return 0
        else:
            return 1
    else:
        return 0