import logging
import configparser
from pathlib import Path
import psycopg2
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

from .extract import get_observed_raw, get_forecast_raw
from .transform import transform_observed, transform_forecast
from .load import load_observed_data, load_forecast_data

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
    2. Fetches list of municipalities
    3. For each municipality: extracts, transforms, and loads weather data
    4. Handles errors and logs failures
    """
    setup_logging()

    # Open database connection and cursor
    conn, cursor = get_connection()

    try:
        # Retrieve municipalities and their codes from the database
        cursor.execute("SELECT municipality_id, postal_code, station_code FROM municipalities;")
        municipalities = cursor.fetchall()

        # Load environment variables (e.g., API key)
        load_dotenv()
        api_key = os.getenv('API_KEY_WEATHER')

        # Target date for observed data: 6 days ago
        target_date = datetime.now() - timedelta(days=6)

        failed_municipalities = []  # Track municipalities where processing fails

        # Process each municipality in the list
        for municipality_id, postal_code, station_code in municipalities:
            
            # --------- Extraction Phase ---------
            # Fetch raw observed data
            raw_observed = get_observed_raw(municipality_id, station_code, target_date, api_key)
            # Fetch raw forecast data
            raw_forecast = get_forecast_raw(municipality_id, postal_code, api_key)

            # --------- Transform Phase ---------
            # Convert raw observed data to structured format
            observed = transform_observed(raw_observed, municipality_id, target_date)
            # Convert raw forecast data to structured format
            forecast = transform_forecast(raw_forecast, municipality_id)

            # --------- Load Phase ---------
            # Initialize flags to track successful insertion
            forecast_loaded = False
            observed_loaded = False

            # Insert forecast data first, then observed data
            if forecast:                            
                load_forecast_data(cursor, forecast)
                forecast_loaded = True
            if observed:
                load_observed_data(cursor, observed)
                observed_loaded = True

            if forecast_loaded and observed_loaded:  
                conn.commit()           
            elif forecast_loaded or observed_loaded:
                # Commit observed or forecast data but flag the municipality as incomplete
                conn.commit()
                loaded = 'FORECAST' if forecast_loaded else 'OBSERVED'
                missing = 'OBSERVED' if forecast_loaded else 'FORECAST'
                logging.error(f"Municipality {municipality_id}: only {loaded} data loaded; {missing} data missing.")
                failed_municipalities.append(municipality_id)
            else:
                # Nothing was loaded
                logging.error( f"Municipality {municipality_id}: no data loaded.")
                failed_municipalities.append(municipality_id)

            # Pause between API calls to respect API rate limits
            time.sleep(10)

        # --------- Final Status Report ---------
        if not failed_municipalities:
            logging.info("All municipalities processed successfully.")
            print("All municipalities processed successfully.")
        else:
            logging.info(f"Failed municipalities: {failed_municipalities}")
            print(f"Failed municipalities: {failed_municipalities}")
            
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Execute the pipeline when the script is run directly
    run_pipeline()
