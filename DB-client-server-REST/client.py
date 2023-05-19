import requests
import glob

url = 'http://localhost:5000/upload'  # Replace with the server URL

path = r'./*.orc'
datasets = glob.glob(path)
#Loopear sobre los datasets
for dataset in datasets:
    # Prepare the file to be uploaded
    files = {'file': open(dataset, 'rb')}

    # Send the POST request to the server
    response = requests.post(url, files=files)

    # Check the response
    if response.status_code == 200:
        print('File uploaded and processed successfully.')
    else:
        print('Error:', response.json()['error'])