import logging
from datetime import datetime, timedelta
import argparse
import database.database as db
from api.weather_api import get_prediction_data, get_meteo_data

logging.basicConfig(filename='logs/weather_data_logs.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("city_id", type=int)

    args = parser.parse_args()

    if 1 <= args.city_id <= 41:
        print(f"City ID {args.city_id} selected")
    else:
        print("Error: The city ID must be between 1 and 41.")
        exit(1)

    city_id = args.city_id

    # Read API key from file
    with open('keys/api.txt', 'r') as file:
        api_key = file.read().strip()

    # Establish database connection
    conn, cursor = db.get_connection()

    city = db.get_city(city_id, conn, cursor)

    postal_code, station_code = city

    # PREDICTION (prediction data of tomorrow)
    prediction_data = get_prediction_data(city_id, postal_code, api_key, conn, cursor)
    db.insert_prediction_data(prediction_data, conn, cursor)

    #Calculate 'date' to get METEO data
    current_date = datetime.now()
    date = current_date - timedelta(days=6)

    # METEO (measured data of 6 days ago)
    meteo_data = get_meteo_data(city_id, station_code, date, api_key, conn, cursor)
    db.insert_meteo_data(meteo_data, conn, cursor)

    conn.commit()

    # Close the database connection
    db.close_connection(conn, cursor)