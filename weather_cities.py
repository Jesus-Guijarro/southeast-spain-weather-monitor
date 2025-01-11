import logging
import time
from datetime import datetime, timedelta
import database.database as db
from api.weather_api import get_prediction_data, get_meteo_data

# Configuración del logging
logging.basicConfig(
    filename='logs/weather_data_logs.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def process_city(city, api_key, conn, cursor, date):
    """
    Processes a city by obtaining and storing forecast and weather data.
    Returns True if the processing was successful, False if there were errors.
    """
    city_id, postal_code, station_code = city

    try:
        prediction_data = get_prediction_data(city_id, postal_code, api_key, conn, cursor)

        meteo_data = get_meteo_data(city_id, station_code, date, api_key, conn, cursor)

        # Verificar que los datos no sean nulos
        if prediction_data and meteo_data:
            db.insert_prediction_data(prediction_data, conn, cursor)
            db.insert_meteo_data(meteo_data, conn, cursor)

            conn.commit()  

            return True
        else:
            logging.error(f"ERROR - City ID: {city_id}")
            return False
    except Exception as e:
        logging.error(f"ERROR - City ID: {city_id}. Info error: {e}")
        return False

def process_cities_with_retries(cities, api_key, conn, cursor, date, retries=3, delay=8):
    """
    Processes a list of cities, handling retries for failed cities.
    :param cities: List of cities (ID, zip code, station code).
    :param retries: Maximum number of retries for failed cities.
    :param delay: Waiting time between API queries (in seconds).
    """
    failed_cities = []

    for city in cities:
        success = process_city(city, api_key, conn, cursor, date)
        if not success:
            failed_cities.append(city)

        time.sleep(delay)  # Pause between queries to avoid API saturation

    for attempt in range(1, retries + 1):
        if not failed_cities:
            break  # Exit if all cities were processed

        print(f"Attempt {attempt} to prosecute failed cities.")

        new_failed_cities = []

        for city in failed_cities:
            success = process_city(city, api_key, conn, cursor, date)

            if not success:
                new_failed_cities.append(city)

            time.sleep(delay) 

        failed_cities = new_failed_cities

    if failed_cities:
        logging.error("The following cities could not be processed after multiple attempts:")
        for city in failed_cities:
            city_id = city[0]
            logging.error(f"ERROR - City ID: {city_id}")
    else:
        logging.info("All cities were successfully processed.")

if __name__ == "__main__":
    # Read API key from file
    with open('keys/api.txt', 'r') as file:
        api_key = file.read().strip()

    # Establish database connection
    conn, cursor = db.get_connection()

    # Get the list of cities from the database
    cities = db.get_cities(conn, cursor)

    #Calculate 'date' to get METEO data
    current_date = datetime.now()
    date = current_date - timedelta(days=6)

    process_cities_with_retries(cities, api_key, conn, cursor, date)

    # Close the database connection
    db.close_connection(conn, cursor)
