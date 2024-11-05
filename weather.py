import requests
from datetime import datetime, timedelta
import logging

import database.database as db

DAYS_METEO = 6

def get_cities():
    """
    Retrieve city stations information from the database.
    """
    conn, cursor = db.get_connection()

    query = "SELECT city_id, postal_code, station_code FROM CITIES;"
    cursor.execute(query)

    city_stations = cursor.fetchall()

    cursor.close()
    conn.close()

    return city_stations

def get_data_url(url, city_id):
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        data = response.json()
        if 'datos' in data:
            return data['datos']
        else:
            logging.error(f"City code: {city_id} Error: No data")
            return None
    except requests.RequestException as e:
        logging.error(f"City code: {city_id} Error: {response.status_code}")
        return None

def get_meteo_data(url, city_id, date):
    data_url = get_data_url(url, city_id)
    if data_url:
        try:
            response = requests.get(data_url)
            response.raise_for_status()
            data = response.json()

            prec = float(data[0].get("prec", "N/A"))

            temperature_avg = round(float(data[0].get("tmed", "N/A").replace(",", ".")))
            temperature_max = round(float(data[0].get("tmax", "N/A").replace(",", ".")))
            temperature_min = round(float(data[0].get("tmin", "N/A").replace(",", ".")))

            humidity_avg = int(data[0].get("hrMedia", "N/A"))
            humidity_max = int(data[0].get("hrMax", "N/A"))
            humidity_min = int(data[0].get("hrMin", "N/A"))


        except requests.RequestException as e:
            logging.error(f"City code: {city_id} Error: Error accessing data URL. {e}")
        except ValueError:
            logging.error(f"City code: {city_id} Error: Error converting response to JSON")
    else:
        logging.error(f"City code: {city_id} Error: Incorrect or unavailable data URL")

def get_prediction_data(url, city_id):
    data_url = get_data_url(url, city_id)
    if data_url:
        try:
            response = requests.get(data_url)
            response.raise_for_status()
            data = response.json()

            prediction  = data[0]["prediccion"]
            precipitations = prediction.get('dia', [{}])[1].get('precipitacion', "N/A")
            prob_precipitation = prediction.get('dia', [{}])[1].get('probPrecipitacion', "N/A")
            prob_storm = prediction.get('dia', [{}])[1].get('probTormenta', "N/A")

            temperatures = prediction.get('dia', [{}])[1].get('temperatura', "N/A")

            values=[]
            for t in temperatures:
                values.append(float(t['value']))

            # Calculate max, min, and average temperature
            temperature_max = max(values)
            temperature_min = min(values)
            temperature_avg = round(float(sum(values)/len(values)))
            
            humidity = prediction.get('dia', [{}])[1].get('humedadRelativa', "N/A")

            values=[]
            for h in humidity:
                values.append(float(h['value']))

            # Calculate max, min, and average humidity
            humidity_max = max(values)
            humidity_min = min(values)
            humidity_avg = round(float(sum(values)/len(values)))
            
            date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


        except requests.RequestException as e:
            logging.error(f"City code: {city_id} Error: Error accessing data URL. {e}")
        except ValueError:
            logging.error(f"City code: {city_id} Error: Error converting response to JSON")
    else:
        logging.error(f"City code: {city_id} Error: Incorrect or unavailable data URL")

def create_api_url_meteo(url_meteo, station_code, date):
    """
    Create and return the API URL for meteorological data based on current date and station code.
    """
    start_date_str = date.strftime("%Y-%m-%dT00:00:00UTC")
    end_date_str = date.strftime("%Y-%m-%dT23:59:59UTC")

    api_url_meteo = url_meteo.format(start_date=start_date_str, end_date=end_date_str, station_code=station_code)

    return api_url_meteo

if __name__ == "__main__":

    logging.basicConfig(filename='logs/city_logs.log', level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Read API key from file
    with open('keys/api.txt', 'r') as file:
        api_key = file.read().strip()

    querystring = {"api_key":api_key}

    headers = {
        'cache-control': "no-cache",
        'accept': "application/json"
        }

    # URLs for API requests
    url_prediction = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{postal_code}"
    url_meteo= "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/estacion/{station_code}"
    
    #cities = get_cities()

    #for cs in cities:
        #city_id, postal_code, station_code = cs

    postal_code, station_code = '03014', '8025'
    city_id = 1

    # PREDICTION (prediction data of tomorrow)
    api_url_prediction = url_prediction.format(postal_code=postal_code)
    get_prediction_data(api_url_prediction, city_id)

    # METEO (measured data of 6 days ago)
    current_date = datetime.now()
    date = current_date - timedelta(days=DAYS_METEO)

    api_url_meteo = create_api_url_meteo(url_meteo, station_code, date)
    #get_meteo_data(api_url_meteo, city_id, date)