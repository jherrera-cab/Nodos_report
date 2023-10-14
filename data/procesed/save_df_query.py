import os
from datetime import datetime

def get_creation_date(file_path):
    return datetime.fromtimestamp(os.path.getctime(file_path))

def check_creation_dates_in_folder(folder_path):
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    creation_dates = [get_creation_date(os.path.join(folder_path, f)).date() for f in files]
        
    date=datetime.today().date()

    # Comprueba si todas las fechas de creación son iguales
    q_file=len(os.listdir(folder_path))
    if q_file >= 1:
        if date != creation_dates[0]:
            for filename in os.listdir(folder_path):
                file=os.path.join(folder_path, filename)
                os.remove(file)
            return 1
        else:
            return 0
    else:
        return 1