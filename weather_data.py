import requests
from datetime import datetime, timedelta
import time
import os
import logging

import database.database as db

# Meteo results from 5 days ago
DAYS_METEO=5

def get_path_folder(query):
    """
    Generate and return the folder path based on the query type ('prediction' or 'meteo') and current date.
    """

    if query=="meteo":
        date = datetime.now() - timedelta(days=DAYS_METEO)
    else:
        date = datetime.now() + timedelta(days=1)

    date_folder_name = date.strftime("%d-%m-%Y")
    folder_path = os.path.join("data", date_folder_name)
    os.makedirs(folder_path, exist_ok=True)

    return folder_path

def get_city_stations():
    """
    Retrieve city stations information from the database.
    """

    conn, cursor = db.get_connection()

    query = "SELECT city_code, station_code, city_name FROM CITY_STATION;"
    cursor.execute(query)

    city_stations = cursor.fetchall()

    cursor.close()
    conn.close()

    return city_stations

def get_data_url(url, city_code, type_query):
    """
    Make a request to the given URL and retrieve JSON data.
    """
    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        data = response.json()
        if 'datos' in data:
            return data['datos']
        else:
            logging.error(f"{type_query} - City code: {city_code}. Error: 'datos' not found in the response")
            return None
    else:
        logging.error(f"{type_query} - City code: {city_code} Error: {response.status_code}")
        return None

def fetch_and_save(url, filename, city_code, type_query):
    """
    Fetch data from the URL and save it as a JSON file.
    """
    data_url = get_data_url(url, city_code, type_query)
    if data_url:
        response = requests.get(data_url)
        if response.status_code == 200:
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(response.text)
        else:
            logging.error(f'{type_query} - City code: {city_code}. Error {response.status_code} when requesting {data_url}')

def create_api_url_meteo(url_meteo, station_code):
    """
    Create and return the API URL for meteorological data based on current date and station code.
    """
    current_date = datetime.now()
    date = current_date - timedelta(days=DAYS_METEO)
        
    start_date_str = date.strftime("%Y-%m-%dT00:00:00UTC")
    end_date_str = date.strftime("%Y-%m-%dT23:59:59UTC")

    api_url_meteo = url_meteo.format(start_date=start_date_str, end_date=end_date_str, station_code=station_code)

    return api_url_meteo

if __name__ == "__main__":
    logging.basicConfig(filename='logs/city_logs.log', level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Read API key from file
    with open('keys/api.txt', 'r') as file:
        api_key = file.read()

    querystring = {"api_key":api_key}

    headers = {
        'cache-control': "no-cache",
        'accept': "application/json"
        }

    # URLs for API requests
    url_prediction = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{city_code}"
    url_meteo= "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/estacion/{station_code}"

    city_stations=get_city_stations()

    for cs in city_stations:
        city_code, station_code, city_name = cs

        # PREDICTION (prediction data of tomorrow)
        api_url_prediction = url_prediction.format(city_code=city_code)
        folder_path = get_path_folder("prediction")
        next_day = (datetime.now() + timedelta(days=1)).strftime('%d-%m-%Y')
        prediction_file_name = f"{folder_path}/{city_code}-prediction-{next_day}.json"
        #fetch_and_save(api_url_prediction, prediction_file_name, city_code, "PREDICTION")

        # METEO (measured data of 5 days ago)
        api_url_meteo = create_api_url_meteo(url_meteo, station_code)
        folder_path = get_path_folder("meteo")
        date_5_days_ago_str = (datetime.now() - timedelta(days=DAYS_METEO)).strftime('%d-%m-%Y')
        meteo_file_name = f"{folder_path}/{city_code}-meteo-{date_5_days_ago_str}.json"
        fetch_and_save(api_url_meteo, meteo_file_name, city_code, "METEO")

        time.sleep(7)   # Wait for 7 seconds between requests