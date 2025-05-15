import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

def get_data_url(url):
    """
    Make a request to the given URL and retrieve JSON data.
    """
    load_dotenv()

    api_key = os.getenv('API_KEY_WEATHER')

    querystring = {"api_key":api_key}

    headers = {
        'cache-control': "no-cache",
        'accept': "application/json"
        }

    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        data = response.json()
        if 'datos' in data:
            return data['datos']
        
def fetch_and_save(url):
    """
    Fetch data from the URL and save it as a JSON file.
    """
    data_url = get_data_url(url)
    if data_url:
        response = requests.get(data_url)
        if response.status_code == 200:
            with open("all_stations.txt", 'w', encoding='utf-8') as f:
                f.write(response.text)

if __name__ == "__main__":

    date = datetime.now() - timedelta(days=4)

    date = date.strftime('%Y-%m-%d')

    fechaIniStr = f"{date}T00:00:00UTC"
    fechaFinStr = f"{date}T23:59:59UTC"

    # URLs for API requests
    url= f"https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{fechaIniStr}/fechafin/{fechaFinStr}/todasestaciones"

    fetch_and_save(url)