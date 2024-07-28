import numpy as np
import nibabel as nib
import os
import logging
import datalad.api as dl
import dropbox
import io
import h5py

from dropbox.files import WriteMode
from dropbox.exceptions import ApiError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# def to_dropbox(file, dbx_path, access_token, chunk_size=16*1024*1024):
def to_dropbox(file, dbx_path, access_token, chunk_size):


    dbx = dropbox.Dropbox(access_token)


    size = file.getbuffer().nbytes
    
    if size <= chunk_size: 
        dbx.files_upload(file.getvalue(), dbx_path, mode=WriteMode("overwrite"))
    else:
        upload_start_result = dbx.files_upload_session_start(file.read(chunk_size))

        # UploadSessionCursor tracks the progress
        cursor = dropbox.files.UploadSessionCursor(session_id=upload_start_result.session_id,
                                                   offset=file.tell())
        

        commit = dropbox.files.CommitInfo(path=dbx_path, mode=WriteMode("overwrite")) 

        while file.tell() < size:
            if (size - file.tell()) <= chunk_size:
                dbx.files_upload_session_finish(file.read(chunk_size),
                                                cursor,
                                                commit)
            else:
                dbx.files_upload_session_append(file.read(chunk_size),
                                                cursor.session_id,
                                                cursor.offset)
                cursor.offset = file.tell()
            
            # Log progress
            progress = (file.tell() / size) * 100
            logging.info(f"Progress: {progress:.2f}%")

    logging.info(f"Uploaded: {dbx_path}")


def process_data(root, app_token):
    dataset = dl.Dataset(root)

    per_task = {}

    subjects = [i for i in os.listdir(root) if i.startswith('sub-')][:10]  # number of subjects to compute

    for i in subjects:

        # ---- needs to be updated for the preprocessed data repo ---- 
        path = os.path.join(root, i)
        func = os.path.join(path, 'func')

        if os.path.exists(func):

            for j in os.listdir(func):
                if j.endswith('bold.nii.gz'):

                    try:
                        path = os.path.join(func, j)
                        task = j.split('task-')[1].split('_')[0]
                        
                        # Use datalad get to retrieve the file content
                        result = dataset.get(path, result_renderer='disabled')

                        if result:
                            img = nib.load(path)
                            data = img.get_fdata()
                            
                            if task not in per_task:
                                per_task[task] = []
                            per_task[task].append(data)
                            
                            logging.info(f"Processed: {path}")
                            
                            # so that files do not get stored locally
                            dataset.drop(path, check=False)
                        else:
                            
                            raise Exception("Error with datalad get")
                    except Exception as e:
                        logging.error(f"Error for file {path}: {str(e)}")


    for k, m in per_task.items():
        try:
            data = np.concatenate(m, axis=-1)
            
            # a bytesIO object needs to hold the h5 file
            h5 = io.BytesIO()

            with h5py.File(h5, 'w') as f:
                f.create_dataset('task_data', data=path, compression='gzip', compression_opts=9)
            h5.seek(0)
        
            dbx_path = f"/task-{k}_bold.h5"
            
            # might want to edit for the preprocessed data 
            chunk_size=16*1024*1024
            to_dropbox(h5, dbx_path, app_token, chunk_size)
            
            logging.info(f"data for task: {k}")
        except Exception as e:

            logging.error(f"Error for task {k}: {str(e)}")

if __name__ == "__main__":
    local_path = os.path.abspath("../ds002345")
    app_token = os.getenv("DROPBOX_APP_TOKEN")

    if not os.path.exists(local_path):
        logging.error(f"{local_path} not found")
        exit(1)
    
    # change to the dataset dir 
    os.chdir(local_path)
    
    process_data('.', app_token)
    logging.info("done")
