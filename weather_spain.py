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

        # PREDICTION (prediction data of tomorrow)
        get_prediction_data(city_id, postal_code, api_key, conn, cursor)

        # METEO (measured data of 6 days ago)
        get_meteo_data(city_id, station_code, date, api_key, conn, cursor)

        conn.commit()

        time.sleep(7)

    # Close the database connection
    db.close_connection(conn, cursor)