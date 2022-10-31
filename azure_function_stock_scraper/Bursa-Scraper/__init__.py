import logging
import requests
import pandas as pd
import string
from bs4 import BeautifulSoup 

import azure.functions as func

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

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        create_data()
        x = 1+1
        return func.HttpResponse(f"This HTTP triggered function successfully {x}.")
    except Exception as e:
        logging.info(e)
        return func.HttpResponse(
                f"!! This HTTP triggered function executed unsuccessfully. \n\t {e} ",
                status_code=200
        )
    
    
