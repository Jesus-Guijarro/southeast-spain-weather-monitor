# pipeline.py

import logging
import configparser
import os
import psycopg2
from datetime import datetime, timedelta
import time

from extract import fetch_meteo_raw, fetch_prediction_raw
from transform import transform_meteo, transform_prediction
from load import insert_meteo_data, insert_prediction_data

def read_db_config():
    """
    Reads database configuration parameters from config.ini file
    """
    cfg = configparser.ConfigParser()
    cfg.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    db = cfg['database']
    return db['dbname'], db['user'], db['password'], db['host'], db['port']


def get_connection():
    """
    Establishes a connection to the PostgreSQL database
    """
    dbname, user, pwd, host, port = read_db_config()
    conn = psycopg2.connect(dbname=dbname, user=user, password=pwd, host=host, port=port)
    return conn, conn.cursor()


def setup_logging():
    logging.basicConfig(
        filename='logs/weather_pipeline.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def run_pipeline():
    setup_logging()

    conn, cursor = get_connection()

    try:
        cursor.execute("SELECT city_id, postal_code, station_code FROM cities;")
        cities = cursor.fetchall()

        with open('keys/api.txt', 'r') as f:
            api_key = f.read().strip()

        target_date = datetime.now() - timedelta(days=6)

        failed_cities = []

        for city_id, postal_code, station_code in cities:
            # Extract
            raw_met = fetch_meteo_raw(city_id, station_code, target_date, api_key)
            raw_pred = fetch_prediction_raw(city_id, postal_code, api_key)

            # Transform
            met = transform_meteo(raw_met, city_id, target_date)
            pred = transform_prediction(raw_pred, city_id)

            # Load
            if met and pred:
                try:
                    insert_prediction_data(cursor, pred)
                    insert_meteo_data(cursor, met)
                    conn.commit()
                    logging.info(f"City {city_id} processed successfully.")
                except Exception as e:
                    logging.error(f"City {city_id} DB error: {e}")
            else:
                logging.error(
                    f"City {city_id} - missing data: "
                    f"met={bool(met)}, pred={bool(pred)}"
                )
                failed_cities.append(city_id)
            time.sleep(10)  # Avoid hitting API rate limits
        if len(failed_cities) == 0:
            logging.info("All cities processed successfully.")
            print("All cities processed successfully.")
        else:
            logging.info(f"Failed cities: {failed_cities}")
            print(f"Failed cities: {failed_cities}")   
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_pipeline()
