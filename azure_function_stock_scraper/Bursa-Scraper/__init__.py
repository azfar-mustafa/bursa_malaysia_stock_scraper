import logging, tempfile, os
import requests
import pandas as pd
import string
from bs4 import BeautifulSoup 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential

import azure.functions as func


def create_container():
        container_name = 'stagings1'
        default_credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url="https://azfarstorageaccountblob.blob.core.windows.net", credential=default_credential)
        container = blob_service_client.create_container(container_name)
        if container:
            print(f"Container {container_name} is created")
        else:
            print(f"Container {container_name} is existed")


def url(letter):
    # Extract data from website
    url = f"https://www.malaysiastock.biz/Listed-Companies.aspx?type=A&value={letter}"
    res = requests.get(url) # Use requests package to get the url content
    soup = BeautifulSoup(res.text, 'html.parser') # Use lxml to parse HTML
    table = soup.find('table', {'class': 'marketWatch'})
    return table

def create_data():
    # Extracted data from website is stored in a dataframe
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


def save_file_to_csv(data):
    filename = 'test.csv'
    local_filepath = tempfile.gettempdir()
    filepath = os.path.join(local_filepath, filename)
    container_name = 'stagings1'
    default_credential = DefaultAzureCredential()

    #processed_file = data.to_csv(index=False, encoding="utf-8")

    #blob_service_client = BlobServiceClient(account_url="https://azfarstorageaccountblob.blob.core.windows.net", credential=default_credential)
    #blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)

    file = open(file=filepath, mode='w')
    file.write("Hello, World!")
    file.close()

    blob_client = BlobServiceClient.get_blob_client(account_url="https://azfarstorageaccountblob.blob.core.windows.net", container_name=container_name, credential=default_credential)

    with open(file=filepath, mode="rb") as data:
        blob_client.upload_blob(data)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        #create_container()
        dataset = create_data()
        save_file_to_csv(dataset)
        x = 1+1
        return func.HttpResponse(f"This HTTP triggered function successfully {x}.")
    except Exception as e:
        logging.info(e)
        return func.HttpResponse(
                f"!! This HTTP triggered function executed unsuccessfully. \n\t {e} ",
                status_code=200
        )
    
    
