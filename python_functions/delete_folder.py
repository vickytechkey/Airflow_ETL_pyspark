import os
import shutil

def delete_folder(folder_name : str):
    if os.path.exists(folder_name):
        shutil.rmtree(folder_name)
        print(f"successfully Deleted the folder : {folder_name}")
    else:
        print(f"Folder {folder_name} is not exists")
    
if __name__ == '__main__':
    delete_folder("./python_functions/male_student")