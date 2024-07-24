import numpy as np 
import nibabel as nib
import os 
import subprocess
from to_dropbox import upload_data 

def process_data(root): 
    data = {}

    for i in os.listdir(root):
        if i.startswith('subject-'):
            func = os.path.join(root, i, 'func')
            for j in os.listdir(func):
                name = j.replace(i.replace('subject-', 'sub-'), '').replace('_bol.nii.gz', '')
                path = os.path.join(func, j)

                if os.path.exists(path):
                    if name not in data: 
                        data[name] = []
                    data[name].apapend(nib.load(path).get_fdata())

    for k in data: 
        data[k] = np.array(data[k])
    return data 

if __name__ == "__main__":
    local_path = "./ds002345"
    app_token = os.getenv("DROPBOX_APP_TOKEN")
    data = process_data(local_path)

    for i, j in data.items(): 

        np.save(f"{i}_data.npy", j)
        print(j.shape)
        # just put in whatever dropbox filename you have
        box_path = f"fmri_data/{i}_data.npy"
        
        upload_data(f"{i}_data.npy", box_path, app_token)




