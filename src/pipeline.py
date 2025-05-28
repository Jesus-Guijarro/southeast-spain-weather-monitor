import logging
import configparser
from pathlib import Path
import psycopg2
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

from .extract import fetch_meteo_raw, fetch_prediction_raw
from .transform import transform_meteo, transform_prediction
from .load import insert_meteo_data, insert_prediction_data

def read_db_config():
    """
    Reads database configuration parameters from config.ini file
    Returns:
        Tuple[str, str, str, str, str]: dbname, user, password, host, port
    """
    cfg = configparser.ConfigParser()

    # Determine project root directory (two levels up from this file)
    project_root = Path(__file__).resolve().parent.parent

    # Read the config file located at project root
    cfg.read(project_root / 'config.ini')
    db = cfg['database']

    # Extract and return connection parameters
    return db['dbname'], db['user'], db['password'], db['host'], db['port']


def get_connection():
    """
    Establishes a connection to the PostgreSQL database
    """
    # Retrieve database credentials
    dbname, user, pwd, host, port = read_db_config()

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(dbname=dbname, user=user, password=pwd, host=host, port=port)

    # Return both connection and cursor for executing queries
    return conn, conn.cursor()


def setup_logging():
    """
    Configures logging to write INFO level and above messages to a log file
    """
    logging.basicConfig(
        filename='logs/pipeline.log',       # Log file path
        level=logging.INFO,                  # Capture info, warning, error, critical
        format='%(asctime)s - %(levelname)s - %(message)s'  # Timestamped format
    )


def run_pipeline():
    """
    Main pipeline orchestration function:
    1. Sets up logging
    2. Fetches list of cities
    3. For each city: extracts, transforms, and loads weather data
    4. Handles errors and logs failures
    """
    setup_logging()

    # Open database connection and cursor
    conn, cursor = get_connection()

    try:
        # Retrieve cities and their codes from the database
        cursor.execute("SELECT city_id, postal_code, station_code FROM cities;")
        cities = cursor.fetchall()

        # Load environment variables (e.g., API key)
        load_dotenv()
        api_key = os.getenv('API_KEY_WEATHER')

        # Target date for historical data: 6 days ago
        target_date = datetime.now() - timedelta(days=6)

        failed_cities = []  # Track cities where processing fails

        # Process each city in the list
        for city_id, postal_code, station_code in cities:
            
            # --------- Extraction Phase ---------
            # Fetch raw meteorological data
            raw_met = fetch_meteo_raw(city_id, station_code, target_date, api_key)
            # Fetch raw prediction data
            raw_pred = fetch_prediction_raw(city_id, postal_code, api_key)

            # --------- Transform Phase ---------
            # Convert raw meteorological data to structured format
            met = transform_meteo(raw_met, city_id, target_date)
            # Convert raw prediction data to structured format
            pred = transform_prediction(raw_pred, city_id)

            # --------- Load Phase ---------
            # Initialize flags to track successful insertion
            pred_loaded = False
            met_loaded  = False

            # Insert prediction data first, then meteorological data
            if pred:                            
                insert_prediction_data(cursor, pred)
                pred_loaded = True
            if met:
                insert_meteo_data(cursor, met)
                met_loaded = True

            if pred_loaded and met_loaded:  
                conn.commit()           
            elif pred_loaded or met_loaded:
                # Commit meteo or prediction data but flag the city as incomplete
                conn.commit()
                loaded = 'PREDICTION' if pred_loaded else 'METEO'
                missing = 'METEO' if pred_loaded else 'PREDICTION'
                logging.warning(f"City {city_id}: only {loaded} data loaded; {missing} data missing.")
                failed_cities.append(city_id)
            else:
                # Nothing was loaded
                logging.error( f"City {city_id}: no data loaded.")
                failed_cities.append(city_id)

            # Pause between API calls to respect API rate limits
            time.sleep(10)

        # --------- Final Status Report ---------
        if not failed_cities:
            logging.info("All cities processed successfully.")
            print("All cities processed successfully.")
        else:
            logging.info(f"Failed cities: {failed_cities}")
            print(f"Failed cities: {failed_cities}")
            
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Execute the pipeline when the script is run directly
    run_pipeline()
