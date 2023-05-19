import requests
import glob
import timeit
import time
import os


url = 'http://localhost:5000/upload'  # Replace with the server URL
import shutil

if os.path.exists("temp.orc"):
    os.remove("temp.orc")

path = r'./*.orc'
datasets = glob.glob(path)
#Loopear sobre los datasets
for dataset in datasets:

    st = time.time()

    # Prepare the file to be uploaded
    files = {'file': open(dataset, 'rb')}

    # Send the POST request to the server
    response = requests.post(url, files=files)

    # Check the response
    if response.status_code == 200:
        print('File uploaded and processed successfully.')
        shutil.move(dataset, "./sent/"+dataset)
    else:
        print('Error:', response.json()['error'])
    
    et = time.time()
    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')