import requests
from datetime import datetime, timedelta
import os
import time
import database as db

def get_path_folder():
    base_directory = "/home/jfgs/Projects/weather-spain"

    current_date = datetime.now()
    date_folder_name = current_date.strftime("%d-%m-%Y")
    folder_path = os.path.join(base_directory, "data", date_folder_name)

    # Create folder
    os.makedirs(folder_path, exist_ok=True)

    return folder_path

def get_city_stations():
    conn, cursor = db.get_connection()

    query = "SELECT city_code, station_code, city_name, station_name FROM CITY_STATION;"

    cursor.execute(query)

    city_stations = cursor.fetchall()

    cursor.close()
    conn.close()

    return city_stations

# Function to make the request and get the URL of the real data
def get_data_url(url):
    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        data = response.json()
        if 'datos' in data:
            return data['datos']
        else:
            print(f"Error: 'datos' not found in the response")
            return None
    else:
        print(f"Error {response.status_code}")
        return None

# Function that obtains the data and saves it in a json file.
def fetch_and_save(url, filename):
    data_url = get_data_url(url)
    if data_url:
        response = requests.get(data_url)
        if response.status_code == 200:
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(response.text)
        else:
            print(f"Error {response.status_code} when requesting {data_url}")

def create_api_url_meteo(url_meteo, station_code):
    current_date = datetime.now()
    date = current_date - timedelta(days=5)
        
    start_date_str = date.strftime("%Y-%m-%dT00:00:00UTC")
    end_date_str = date.strftime("%Y-%m-%dT23:59:59UTC")

    api_url_meteo = url_meteo.format(start_date=start_date_str, end_date=end_date_str, station_code=station_code)

    return api_url_meteo

if __name__ == "__main__":

    # API key, querystring and headers
    with open('keys/api.txt', 'r') as file:
        api_key = file.read()

    querystring = {"api_key":api_key}

    headers = {
        'cache-control': "no-cache",
        'accept': "application/json"
        }

    # URLs API requests
    url_prediction = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{city_code}"
    url_meteo= "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/estacion/{station_code}"

    folder_path = get_path_folder()

    city_stations=get_city_stations()

    for cs in city_stations:
        city_code, station_code, city_name, station_name = cs

        print(city_name)
        
        api_url_prediction = url_prediction.format(city_code=city_code)

        prediction_file_name = f"{folder_path}/predic-{datetime.now().strftime('%d-%m-%Y')}-{city_code}.json"

        fetch_and_save(api_url_prediction, prediction_file_name)

        print(station_name)

        api_url_meteo = create_api_url_meteo(url_meteo, station_code)

        meteo_file_name = f"{folder_path}/meteo-{datetime.now().strftime('%d-%m-%Y')}-{city_code}.json"

        fetch_and_save(api_url_meteo, meteo_file_name)

        time.sleep(5)


