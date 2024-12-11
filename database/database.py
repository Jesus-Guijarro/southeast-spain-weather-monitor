import configparser
import os
import psycopg2
import json

def read_db_config():
    """
    Reads database configuration parameters from config.ini file
    """
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.ini')
    config.read(config_path)

    # Extract database configuration parameters
    db_config = config['database']

    return {
        'dbname': db_config['dbname'],
        'user': db_config['user'],
        'password': db_config['password'],
        'host': db_config['host'],
        'port': db_config['port']
    }

def get_connection():
    """
    Establishes a connection to the PostgreSQL database
    """
    config = read_db_config()

    try:
        conn = psycopg2.connect(
            dbname=config['dbname'],
            user=config['user'],
            password=config['password'],
            host=config['host'],
            port=config['port']
        )
        cursor = conn.cursor()
        return conn, cursor
    except psycopg2.Error as e:
        raise Exception(f"Error connecting to the database: {e}")
    
def close_connection(conn, cursor):
    """
    Closes the database connection and cursor.
    """
    try:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    except psycopg2.Error as e:
        raise Exception(f"Error closing the connection to the database: {e}")

def get_city(city_id, conn, cursor):
    """
    Gets information about a city by its ID.
    """
    query = "SELECT postal_code, station_code FROM CITIES WHERE city_id = %s;"
    try:
        cursor.execute(query, (city_id,))
        city = cursor.fetchone()
        if city:
            return city
        else:
            raise ValueError(f"City with ID {city_id} not found")
    except psycopg2.Error as e:
        raise Exception(f"Error when obtaining the city: {e}")

def get_cities(conn, cursor):
    """
    Retrieve all cities information from the database.
    """
    query = "SELECT city_id, postal_code, station_code FROM CITIES;"
    try:
        cursor.execute(query)
        cities = cursor.fetchall()
        return cities
    except psycopg2.Error as e:
        raise Exception(f"Error in obtaining cities: {e}")

def insert_meteo_data(meteo_data, conn, cursor):
    # Insert meteo data in WEATHER_DATA table
    update_query = """
    UPDATE WEATHER_DATA
    SET
        temperature_measured_avg = %s,
        temperature_measured_max = %s,
        temperature_measured_min = %s,
        humidity_measured_avg = %s,
        humidity_measured_max = %s,
        humidity_measured_min = %s,
        precipitation = %s
    WHERE city_id = %s AND date = %s;
    """
    cursor.execute(update_query, (
        meteo_data['temperature_avg'], meteo_data['temperature_max'], meteo_data['temperature_min'],
        meteo_data['humidity_avg'], meteo_data['humidity_max'], meteo_data['humidity_min'], 
        meteo_data['precipitation'], meteo_data['city_id'],meteo_data['date']
    ))

def insert_prediction_data(prediction_data, conn, cursor):
    # Insert prediction data into WEATHER_DATA table
    insert_query = """
    INSERT INTO WEATHER_DATA (
        city_id, date, temperature_predicted_max, temperature_predicted_min, temperature_predicted_avg,
        humidity_predicted_avg, humidity_predicted_max, humidity_predicted_min, precipitations, 
        prob_precipitation, prob_storm
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (city_id, date) DO NOTHING;
    """
    cursor.execute(insert_query, (prediction_data['city_id'], prediction_data['date'], prediction_data['temperature_max'], prediction_data['temperature_min'], 
                                    prediction_data['temperature_avg'], prediction_data['humidity_max'], prediction_data['humidity_min'], prediction_data['humidity_avg'], 
                                    json.dumps(prediction_data['precipitations']), json.dumps(prediction_data['prob_precipitation']), json.dumps(prediction_data['prob_storm'])))


                                    