import requests
from datetime import datetime, timedelta
import sys

def create_api_url_meteo(url_meteo, station_code, date):
    """
    Create and return the API URL for meteorological data based on current date and station code.
    """
    start_date_str = date.strftime("%Y-%m-%dT00:00:00UTC")
    end_date_str = date.strftime("%Y-%m-%dT23:59:59UTC")

    api_url_meteo = url_meteo.format(start_date=start_date_str, end_date=end_date_str, station_code=station_code)

    return api_url_meteo

def get_data_url(url, api_key):
    """
    Returns weather data from the URL returned by the API
    """
    querystring = {"api_key":api_key}

    headers = {
        'cache-control': "no-cache",
        'accept': "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        data = response.json()
        if 'datos' in data:
            return data['datos']
        else:
            return None
    except requests.exceptions.RequestException as e:
        if response is not None:
            print(f"Error HTTP: {response.status_code}")
        else:
            print("No response received.")
        return None

def get_meteo_data(station_code, date, api_key):
    """
    Returns 'meteo' data in JSON format
    """
    url_meteo= "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/estacion/{station_code}"
    api_url_meteo = create_api_url_meteo(url_meteo, station_code, date)

    data_url = get_data_url(api_url_meteo, api_key)

    if data_url:
        try:
            response = requests.get(data_url)
            
            response.raise_for_status()
            data = response.json()
        except:
            print("ERROR JSON")
    return data

if __name__ == "__main__":
    with open('../keys/api.txt', 'r') as file:
        api_key = file.read().strip()

    #Calculate 'date' to get METEO data
    current_date = datetime.now()
    date = current_date - timedelta(days=6)

    list_stations = ['8050X']

    with open("output_test.txt", "w") as f:
        sys.stdout = f
        for station_code in list_stations:
            print(f"GET DATA FROM {station_code}")
            meteo_data = get_meteo_data(station_code, date, api_key)
            print(meteo_data)