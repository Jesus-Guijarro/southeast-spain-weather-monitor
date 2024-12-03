import logging
import requests
from datetime import datetime, timedelta
import database.database as db
from api.weather_api import get_prediction_data, get_meteo_data


def test_city_data(city_id):
    with open('keys/api.txt', 'r') as file:
        api_key = file.read().strip()

    conn, cursor = db.get_connection()

    city = db.get_city(city_id, conn, cursor)
    
    db.close_connection(conn, cursor)

if __name__ == "__main__":
    test_city_data(38)  
