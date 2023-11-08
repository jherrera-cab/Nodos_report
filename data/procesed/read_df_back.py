import os
import pandas as pd
from pathlib import Path
from print_test import print_test

def read_df_back():
    def list_files_in_folder(folder_path):
        if not os.path.exists(folder_path):
            print(f"La carpeta '{folder_path}' no existe.")
            return []

        if not os.path.isdir(folder_path):
            print(f"'{folder_path}' no es una carpeta.")
            return []

        file_paths = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

        return file_paths
    raiz=Path(__file__).resolve().parents[1]
   
    folder_path = os.path.join(raiz,'procesed\df')

    file_paths = list_files_in_folder(folder_path)
    
    dic_df={}
    for file in file_paths:
        name_file=os.path.splitext(os.path.basename(file))
        df = pd.read_csv(file)
        dic_df[name_file[0]]=df

    return(dic_df)