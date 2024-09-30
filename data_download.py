import requests
import concurrent.futures
import os

folder_path = 'your_folder_path'
file = "your_file_path"
url = "your_link"
chunksize = 1024

# Check if the folder exists
if not os.path.exists(folder_path):
    # Create the folder
    os.makedirs(folder_path)

def download(link, filelocation):
    r = requests.get(link, stream=True)
    with open(filelocation, 'wb') as f:
        #prevent loading the entire response into memory at once
        for chunk in r.iter_content(chunksize):
            if chunk:
                f.write(chunk)

# Using ThreadPoolExecutor to download files concurrently
with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.submit(download, url, file)
