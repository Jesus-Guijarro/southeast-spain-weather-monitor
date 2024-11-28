import requests
from datetime import datetime, timedelta
import time
import logging
import json
import database.database as db

logging.basicConfig(filename='logs/city_logs.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def get_cities(conn, cursor):
    """
    Retrieve city stations information from the database.
    """
    query = "SELECT city_id, postal_code, station_code FROM CITIES;"
    cursor.execute(query)

    city_stations = cursor.fetchall()

    return city_stations

if __name__ == "__main__":
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
    
    # Establish database connection
    conn, cursor = db.get_connection()

    cities = get_cities(conn, cursor)

    for c in cities:
        city_id, postal_code, station_code = c

        # PREDICTION (prediction data of tomorrow)
        api_url_prediction = url_prediction.format(postal_code=postal_code)
        get_prediction_data(api_url_prediction, city_id, conn, cursor)

        # METEO (measured data of 6 days ago)
        current_date = datetime.now()
        date = current_date - timedelta(days=DAYS_METEO)

        api_url_meteo = create_api_url_meteo(url_meteo, station_code, date)
        get_meteo_data(api_url_meteo, city_id, date, conn, cursor)

        time.sleep(7)

    # Close the database connection
    db.close_connection(conn, cursor)