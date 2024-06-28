
from datetime import datetime
import os

def get_path_folder():
    current_date = datetime.now()
    date_folder_name = current_date.strftime("%d-%m-%Y")
    folder_path = os.path.join("aaaa", date_folder_name)

    # Create folder
    os.makedirs(folder_path, exist_ok=True)

    return folder_path

get_path_folder()