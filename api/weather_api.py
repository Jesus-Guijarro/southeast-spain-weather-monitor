import requests
import logging
import json
from datetime import datetime, timedelta
import database.database as db

def create_api_url_meteo(url_meteo, station_code, date):
    """
    Create and return the API URL for meteorological data based on current date and station code.
    """
    start_date_str = date.strftime("%Y-%m-%dT00:00:00UTC")
    end_date_str = date.strftime("%Y-%m-%dT23:59:59UTC")

    api_url_meteo = url_meteo.format(start_date=start_date_str, end_date=end_date_str, station_code=station_code)

    return api_url_meteo

def get_data_url(url, city_id, api_key, type_query):

    querystring = {"api_key":api_key}

    headers = {
        'cache-control': "no-cache",
        'accept': "application/json"
    }

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

def get_meteo_data(city_id, station_code, date, api_key, conn, cursor):
    url_meteo= "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/estacion/{station_code}"
    api_url_meteo = create_api_url_meteo(url_meteo, station_code, date)

    data_url = get_data_url(api_url_meteo, city_id, api_key, "METEO")
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
                temperature_avg, temperature_max, temperature_min,
                humidity_avg, humidity_max, humidity_min, precipitation,
                city_id, date
            ))
            if cursor.rowcount == 0:
                insert_query = """
                INSERT INTO WEATHER_DATA (
                    city_id, date, temperature_measured_avg, temperature_measured_max, 
                    temperature_measured_min, humidity_measured_avg, 
                    humidity_measured_max, humidity_measured_min, precipitation
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, (
                    city_id, date, temperature_avg, temperature_max, temperature_min,
                    humidity_avg, humidity_max, humidity_min, precipitation
                ))

        except requests.RequestException:
            logging.error(f"METEO - City code: {city_id} Error: error accessing data URL.")
        except ValueError:
            logging.error(f"METEO - City code: {city_id} Error: error converting response to JSON.")
    else:
        logging.error(f"METEO - City code: {city_id} Error: incorrect or unavailable data URL")

def get_prediction_data(city_id, postal_code, api_key, conn, cursor):
    url_prediction = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{postal_code}"
    api_url_prediction = url_prediction.format(postal_code=postal_code)

    data_url = get_data_url(api_url_prediction, city_id, api_key, "PREDICTION")
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

        except requests.RequestException:
            logging.error(f"PREDICTION - City code: {city_id} Error: error accessing data URL.")
        except ValueError:
            logging.error(f"PREDICTION - City code: {city_id} Error: error converting response to JSON")
    else:
        logging.error(f"PREDICTION - City code: {city_id} Error: incorrect or unavailable data URL")