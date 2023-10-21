import os
import pandas as pd

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

    folder_path = r'C:\Users\jherrera.FINANCREDITOS\OneDrive - Financreditos S.A.S\NodosLab\Nodos_Report\data\procesed\df'

    file_paths = list_files_in_folder(folder_path)

    dic_df={}
    for file in file_paths:
        name_file=os.path.splitext(os.path.basename(file))
        df = pd.read_csv(file)
        dic_df[name_file[0]]=df
        
    return(dic_df)