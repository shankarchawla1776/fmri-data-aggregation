import dropbox
import os 

def upload_data(filepath, box_path, app_token):
    box = dropbox.Dropbox(app_token)
    with open(filepath, 'rb') as f: 
        box.files_upload(f.read(), box_path, mode=dropbox.files.WriteMode.overwrite)

