import datalad.api as dl 
from main import process_data 

def datalad_init(url, local_path):
    data = dl.install(path=local_path, source=url)
    data.get('*/func/*bold.nii.gz')

    return data 

if __name__ == "__main__":
    url = "https://github.com/OpenNeuroDatasets/ds002345"
    local_path = "./ds002345"

    dat = datalad_init(url, local_path)

    print("files loaded")
