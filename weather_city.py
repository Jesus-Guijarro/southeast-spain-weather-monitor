import logging
from datetime import datetime, timedelta
import database.database as db
from api.weather_api import get_prediction_data, get_meteo_data

logging.basicConfig(filename='logs/weather_data_logs.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

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

if __name__ == "__main__":

    try:
        city_id = int(input("Enter a city ID between 1 and 41: "))

        if 1 <= city_id <= 41:
            print(f"City ID {city_id} selected")
        else:
            print("Error: The city ID must be between 1 and 41.")
            exit(1)
    except ValueError:
        print("Error: The input must be an integer.")
        exit(1)

    # Read API key from file
    with open('keys/api.txt', 'r') as file:
        api_key = file.read().strip()

    # Establish database connection
    conn, cursor = db.get_connection()

    city = db.get_city(city_id, conn, cursor)
    postal_code, station_code = city
    city = city_id, postal_code, station_code

    #Calculate 'date' to get METEO data
    date = datetime.now() - timedelta(days=6)

    success = process_city(city, api_key, conn, cursor, date)
    if not success:
        print(f"ERROR - City ID: {city_id}")
    else:
        print(f"INFO - City ID {city_id} successfully processed.")

    # Close the database connection
    db.close_connection(conn, cursor)