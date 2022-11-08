import logging, tempfile, os, pendulum
import requests
import pandas as pd
import string
from bs4 import BeautifulSoup

import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential



# This function is to create container in blob storage
def create_container(container, storage_account_url):
        default_credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=default_credential)
        blob_container = blob_service_client.create_container(container)
        if blob_container:
            print(f"Container {container} is created")
        else:
            print(f"Container {container} is existed")


# This function is to extract data from bursa stock malaysia website
def url(letter):
    url = f"https://www.malaysiastock.biz/Listed-Companies.aspx?type=A&value={letter}"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    table = soup.find('table', {'class': 'marketWatch'})
    return table


# This function is to store the extracted website data in a dataframe
def create_data():
    alphabet_param = list(string.ascii_lowercase)
    number_param = ['0']
    actual_param = alphabet_param + number_param
    headers = ['company_code', 'company_name', 'shariah', 'unknown' ,'sector', 'market_cap', 'last_price', 'pe', 'dy', 'roe']
    df = pd.DataFrame(columns = headers)

    for i in actual_param:
        bursa_table = url(i)
        for row in bursa_table.find_all('tr')[1:]:
            first_part = row.find_all('h3')
            first_data = [h3.text.strip() for h3 in first_part][:2]
            second_part = row.find_all('img')
            second_data = [td['src'] for td in second_part]
            third_part = row.find_all('td')
            third_data = [td.text.strip() for td in third_part][1:]
            complete_data = first_data + second_data + third_data
            length = len(df)
            df.loc[length] = complete_data
        
    return df


# This function is to upload dataframe into blob
def upload_file_to_blob(data, container, filename, storage_account_url):
    local_filepath = tempfile.gettempdir()
    filepath = os.path.join(local_filepath, filename)
    default_credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=default_credential)
    blob_client = blob_service_client.get_container_client(container=container)

    data.to_csv(filepath, index=False)
    logging.info('File is created')

    with open(filepath, "rb") as data:
        blob_client.upload_blob(name=filename, data=data)
        logging.info('File is uploaded')


# Main function to accept to execute the process
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    account_url = "https://azfarstorageaccountblob.blob.core.windows.net"
    container_name = 'raw'
    current_date = pendulum.today('Asia/Kuala_Lumpur')
    current_date = current_date.format('DDMMYYYY')
    filename = f"raw_bursa_malaysia_{current_date}.csv"

    try:
        create_container(container_name, account_url)
        dataset = create_data()
        upload_file_to_blob(dataset, container_name, filename, account_url)
        return func.HttpResponse(f"{filename} is successfully uploaded.")
    except Exception as e:
        logging.info(e)
        #status_code = 
        return func.HttpResponse(
                f"!! This HTTP triggered function executed unsuccessfully. \n\t {e} ",
                status_code=200
        )
    
    
