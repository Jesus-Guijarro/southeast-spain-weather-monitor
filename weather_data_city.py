import requests
from datetime import datetime, timedelta
import logging
import json
import sys
import database.database as db

DAYS_METEO = 6

def get_data_url(url, city_id, type_query):
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        data = response.json()
        if 'datos' in data:
            return data['datos']
        else:
            logging.error(f"{type_query} - City code: {city_id} Error: No data")
            return None
    except requests.RequestException as e:
        logging.error(f"{type_query} - City code: {city_id} Error: {response.status_code}")
        return None

def get_meteo_data(url, city_id, date, conn, cursor):
    data_url = get_data_url(url, city_id, "METEO")
    if data_url:
        try:
            response = requests.get(data_url)
            response.raise_for_status()
            data = response.json()

            precipitation = float(data[0].get("prec", "None").replace(",", "."))

            temperature_avg = round(float(data[0].get("tmed", "None").replace(",", ".")))
            temperature_max = round(float(data[0].get("tmax", "None").replace(",", ".")))
            temperature_min = round(float(data[0].get("tmin", "None").replace(",", ".")))

            humidity_avg = int(data[0].get("hrMedia", "None"))
            humidity_max = int(data[0].get("hrMax", "None"))
            humidity_min = int(data[0].get("hrMin", "None"))

            # Update data in WEATHER_DATA table
            insert_or_update_query = """
            INSERT INTO WEATHER_DATA (city_id, date, temperature_measured_avg, temperature_measured_max, 
                                    temperature_measured_min, humidity_measured_avg, 
                                    humidity_measured_max, humidity_measured_min, precipitation)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city_id, date)
            DO UPDATE SET 
                temperature_measured_avg = EXCLUDED.temperature_measured_avg,
                temperature_measured_max = EXCLUDED.temperature_measured_max,
                temperature_measured_min = EXCLUDED.temperature_measured_min,
                humidity_measured_avg = EXCLUDED.humidity_measured_avg,
                humidity_measured_max = EXCLUDED.humidity_measured_max,
                humidity_measured_min = EXCLUDED.humidity_measured_min,
                precipitation = EXCLUDED.precipitation;
            """

            cursor.execute(insert_or_update_query, (city_id, date, temperature_avg, temperature_max, temperature_min,
                                        humidity_avg, humidity_max, humidity_min, precipitation))

            # Commit the transaction
            conn.commit()

        except requests.RequestException:
            logging.error(f"METEO - City code: {city_id} Error: error accessing data URL.")
        except ValueError:
            logging.error(f"METEO - City code: {city_id} Error: error converting response to JSON.")
    else:
        logging.error(f"METEO - City code: {city_id} Error: incorrect or unavailable data URL")

def get_prediction_data(url, city_id, conn, cursor):
    data_url = get_data_url(url, city_id, "PREDICTION")
    if data_url:
        try:
            response = requests.get(data_url)
            response.raise_for_status()
            data = response.json()

            prediction  = data[0]["prediccion"]
            precipitations = prediction.get('dia', [{}])[1].get('precipitacion', "None")
            prob_precipitation = prediction.get('dia', [{}])[1].get('probPrecipitacion', "None")
            prob_storm = prediction.get('dia', [{}])[1].get('probTormenta', "None")

            temperatures = prediction.get('dia', [{}])[1].get('temperatura', "None")

            values=[]
            for t in temperatures:
                values.append(float(t['value']))

            # Calculate max, min, and average temperature
            temperature_max = max(values)
            temperature_min = min(values)
            temperature_avg = round(float(sum(values)/len(values)))
            
            humidity = prediction.get('dia', [{}])[1].get('humedadRelativa', "None")

            values=[]
            for h in humidity:
                values.append(float(h['value']))

            # Calculate max, min, and average humidity
            humidity_max = max(values)
            humidity_min = min(values)
            humidity_avg = round(float(sum(values)/len(values)))
            
            date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

            # Insert data into WEATHER_DATA table
            insert_query = """
            INSERT INTO WEATHER_DATA (
                city_id, date, temperature_predicted_max, temperature_predicted_min, temperature_predicted_avg,
                humidity_predicted_avg, humidity_predicted_max, humidity_predicted_min, precipitations, 
                prob_precipitation, prob_storm
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city_id, date) DO NOTHING;
            """
            cursor.execute(insert_query, (city_id, date, temperature_max, temperature_min, temperature_avg,
                                          humidity_max, humidity_min, humidity_avg, 
                                          json.dumps(precipitations), json.dumps(prob_precipitation), 
                                          json.dumps(prob_storm)))

            # Commit the transaction
            conn.commit()

        except requests.RequestException:
            logging.error(f"PREDICTION - City code: {city_id} Error: error accessing data URL.")
        except ValueError:
            logging.error(f"PREDICTION - City code: {city_id} Error: error converting response to JSON")
    else:
        logging.error(f"PREDICTION - City code: {city_id} Error: incorrect or unavailable data URL")

def create_api_url_meteo(url_meteo, station_code, date):
    """
    Create and return the API URL for meteorological data based on current date and station code.
    """
    start_date_str = date.strftime("%Y-%m-%dT00:00:00UTC")
    end_date_str = date.strftime("%Y-%m-%dT23:59:59UTC")

    api_url_meteo = url_meteo.format(start_date=start_date_str, end_date=end_date_str, station_code=station_code)

    return api_url_meteo