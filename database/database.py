import configparser
import os
import psycopg2

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

def insert_meteo_data():
    print("meteo")

def insert_prediction_data():
    print("prediction")