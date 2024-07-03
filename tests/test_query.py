
from datetime import datetime, timedelta
import requests
import database as db


def get_city_stations():
    conn, cursor = db.get_connection()

    query = "SELECT city_code, station_code, city_name, station_name FROM CITY_STATION;"

    cursor.execute(query)

    city_stations = cursor.fetchall()

    cursor.close()
    conn.close()

    return city_stations

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

def fetch(url):
    data_url = get_data_url(url)
    if data_url:
        response = requests.get(data_url)
        if response.status_code == 200:
            print("DATA")
        else:
            print(f"Error {response.status_code} when requesting {data_url}")

def create_api_url_meteo(url_meteo, station_code):
    current_date = datetime.now()
    date = current_date - timedelta(days=5)
        
    start_date_str = date.strftime("%Y-%m-%dT00:00:00UTC")
    end_date_str = date.strftime("%Y-%m-%dT23:59:59UTC")

    api_url_meteo = url_meteo.format(start_date=start_date_str, end_date=end_date_str, station_code=station_code)

    return api_url_meteo

city_stations=get_city_stations()

if __name__ == "__main__":

    url_prediction = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{city_code}"
    url_meteo= "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/estacion/{station_code}"

    with open('keys/api.txt', 'r') as file:
        api_key = file.read()

    querystring = {"api_key":api_key}

    headers = {
        'cache-control': "no-cache",
        'accept': "application/json"
        }

    #station to test
    station_code = "8500A"

    api_url_meteo=create_api_url_meteo(url_meteo,station_code)
    fetch(api_url_meteo)

    '''
    #City to test
    city_code = '12040'

    #api_url_prediction = url_prediction.format(city_code=city_code)
    #fetch(api_url_prediction)
    '''
