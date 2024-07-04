import os
from airflow.models import Variable
import requests

API_KEY = Variable.get("api_key")
URL_DRIVER_REQUESTS = Variable.get("url_driver_requests")
URL_DRIVER_DOWNLOAD = Variable.get("url_driver_download")

## Local path
TEMP_PATH = Variable.get("temp_path")


def download_file_drive(folder_name: str, folder_id: str) -> str:
    url = URL_DRIVER_REQUESTS.format(folder_id, API_KEY)
    response = requests.get(url)

    response.raise_for_status()
    
    files = response.json().get('files', [])
    
    if not files:
        print('No files found.')
        return
    latest_file = files[0]
    file_id = latest_file['id']
    file_name = latest_file['name']

    download_url = URL_DRIVER_DOWNLOAD.format(file_id, API_KEY)
    download_response = requests.get(download_url)
    download_response.raise_for_status()
    file_local = os.path.join(TEMP_PATH, folder_name, file_name)
    os.makedirs(os.path.dirname(file_local), exist_ok=True)
    with open(file_local, 'wb') as f:
        f.write(download_response.content)
    print(f"Downloaded {file_name}")
    return file_local
