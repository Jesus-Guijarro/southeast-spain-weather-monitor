import logging
import time
from datetime import datetime, timedelta
import database.database as db
from api.weather_api import get_prediction_data, get_meteo_data

logging.basicConfig(filename='logs/weather_data_logs.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    # Read API key from file
    with open('keys/api.txt', 'r') as file:
        api_key = file.read().strip()

    # Establish database connection
    conn, cursor = db.get_connection()

    cities = db.get_cities(conn, cursor)

    #Calculate 'date' to get METEO data
    current_date = datetime.now()
    date = current_date - timedelta(days=6)

    for city in cities:
        city_id, postal_code, station_code = city

        prediction_data = get_prediction_data(city_id, postal_code, api_key, conn, cursor)

        meteo_data = get_meteo_data(city_id, station_code, date, api_key, conn, cursor)

        if prediction_data and meteo_data:
            db.insert_prediction_data(prediction_data, conn, cursor)

            db.insert_meteo_data(meteo_data, conn, cursor)
        else:  
            print(f"ERROR - City ID: {city_id}")

        conn.commit()

        time.sleep(8)

    # Close the database connection
    db.close_connection(conn, cursor)