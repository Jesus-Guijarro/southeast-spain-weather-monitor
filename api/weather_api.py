import requests
import logging
from datetime import datetime, timedelta

def create_api_url_meteo(url_meteo, station_code, date):
    """
    Create and return the API URL for meteorological data based on current date and station code.
    """
    start_date_str = date.strftime("%Y-%m-%dT00:00:00UTC")
    end_date_str = date.strftime("%Y-%m-%dT23:59:59UTC")

    api_url_meteo = url_meteo.format(start_date=start_date_str, end_date=end_date_str, station_code=station_code)

    return api_url_meteo

def convert_to_float(value):
    """
    Function to convert strings to floats
    """
    if value is not None and value != "Ip":
        return round(float(value.replace(",", "."))) if isinstance(value, str) else round(float(value))
    elif value == "Ip":
        return None
    return value

def convert_to_int(value):
    """
    Function to convert strings to integers
    """
    if value is not None and value != "Ip":
        return int(value)
    elif value == "Ip":
        return None
    return value

def get_data_url(url, city_id, api_key, type_query):
    """
    Returns weather data from the URL returned by the API
    """
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
    except requests.exceptions.RequestException as e:
        logging.error(f"{type_query} - City code: {city_id} No response received.")
        return None

def get_meteo_data(city_id, station_code, date, api_key):
    """
    Returns 'meteo' data in JSON format
    """
    url_meteo= "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/estacion/{station_code}"
    api_url_meteo = create_api_url_meteo(url_meteo, station_code, date)

    data_url = get_data_url(api_url_meteo, city_id, api_key, "METEO")
    if data_url:
        try:
            response = requests.get(data_url)
            response.raise_for_status()
            data = response.json()

            precipitation = convert_to_float(data[0].get("prec", None))

            temperature_avg = convert_to_float(data[0].get("tmed", None))
            temperature_max = convert_to_float(data[0].get("tmax", None))
            temperature_min = convert_to_float(data[0].get("tmin", None))

            humidity_avg = convert_to_int(data[0].get("hrMedia", None))
            humidity_max = convert_to_int(data[0].get("hrMax", None))
            humidity_min = convert_to_int(data[0].get("hrMin", None))

            date = date.strftime("%Y-%m-%d")

            return {
                "city_id": city_id,
                "date": date,
                "precipitation": precipitation,
                "temperature_avg": temperature_avg,
                "temperature_max": temperature_max,
                "temperature_min": temperature_min,
                "humidity_avg": humidity_avg,
                "humidity_max": humidity_max,
                "humidity_min": humidity_min
            }
            
        except requests.exceptions.RequestException:
            logging.error(f"METEO - City code: {city_id} Error: error accessing data URL.")
            return None
        except ValueError:
            logging.error(f"METEO - City code: {city_id} Error: error converting response to JSON.")
            return None
    else:
        logging.error(f"METEO - City code: {city_id} Error: incorrect or unavailable data URL")
        return None

def get_prediction_data(city_id, postal_code, api_key):
    """
    Returns 'prediction' data in JSON format
    """
    url_prediction = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{postal_code}"
    api_url_prediction = url_prediction.format(postal_code=postal_code)

    data_url = get_data_url(api_url_prediction, city_id, api_key, "PREDICTION")
    if data_url:
        try:
            response = requests.get(data_url)
            response.raise_for_status()
            data = response.json()

            prediction  = data[0]["prediccion"]

            precipitations = prediction.get('dia', [{}])[1].get('precipitacion', None)
            prob_precipitation = prediction.get('dia', [{}])[1].get('probPrecipitacion', None)
            prob_storm = prediction.get('dia', [{}])[1].get('probTormenta', None)

            temperatures = prediction.get('dia', [{}])[1].get('temperatura', None)
            if temperatures:
                values=[]
                for t in temperatures:
                    values.append(float(t['value']))

                # Calculate max, min, and average temperature
                temperature_max = max(values)
                temperature_min = min(values)
                temperature_avg = round(float(sum(values)/len(values)))
            
            humidity = prediction.get('dia', [{}])[1].get('humedadRelativa', None)
            if humidity:
                values=[]
                for h in humidity:
                    values.append(float(h['value']))

                # Calculate max, min, and average humidity
                humidity_max = max(values)
                humidity_min = min(values)
                humidity_avg = round(float(sum(values)/len(values)))
            
            date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

            return {
                "city_id": city_id,
                "date": date,
                "temperature_max": temperature_max,
                "temperature_min": temperature_min,
                "temperature_avg": temperature_avg,
                "humidity_avg": humidity_avg,
                "humidity_max": humidity_max,
                "humidity_min": humidity_min,
                "precipitations": precipitations,
                "prob_precipitation": prob_precipitation,
                "prob_storm": prob_storm,
            }
        except requests.exceptions.RequestException:
            logging.error(f"PREDICTION - City code: {city_id} Error: error accessing data URL.")
            return None
        except ValueError:
            logging.error(f"PREDICTION - City code: {city_id} Error: error converting response to JSON")
            return None
    else:
        logging.error(f"PREDICTION - City code: {city_id} Error: incorrect or unavailable data URL")
        return None