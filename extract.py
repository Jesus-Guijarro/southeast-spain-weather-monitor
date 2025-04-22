import requests
import logging
import time

# HTTP status codes that should trigger a retry
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
# Maximum number of retry attempts
MAX_RETRIES = 5
# Delay between retries in seconds
DELAY = 10

def _get_data_url(endpoint_url, city_id, api_key, query_type):
    """
    Internal: gets the actual data URL from AEMET 'datos' field
    """
    querystring = {"api_key": api_key}
    headers = {"cache-control": "no-cache", "accept": "application/json"}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(endpoint_url, headers=headers, params=querystring)
            if response.status_code in RETRY_STATUS_CODES:
                logging.warning(
                    f"{query_type} - City {city_id} - status {response.status_code}. "
                    f"Attempt {attempt}/{MAX_RETRIES}"
                )
                if attempt < MAX_RETRIES:
                    time.sleep(DELAY)
                    continue
            response.raise_for_status()

            data = response.json()
            if 'datos' in data:
                return data['datos']
            else:
                logging.error(f"{query_type} - City {city_id} - 'datos' key missing in response")
                return None

        except requests.RequestException as e:
            status = getattr(e.response, 'status_code', None)
            if status in RETRY_STATUS_CODES and attempt < MAX_RETRIES:
                logging.warning(
                    f"{query_type} - City {city_id} - request error {status or ''}: {e}. "
                    f"Attempt {attempt}/{MAX_RETRIES}"
                )
                time.sleep(DELAY)
                continue
            logging.error(f"{query_type} - City {city_id} - request error final: {e}")
            return None
    return None


def fetch_meteo_raw(city_id, station_code, date, api_key):
    """
    Fetches raw meteo JSON from AEMET for a given station and date.
    """
    template = (
        "https://opendata.aemet.es/opendata/api/"
        "valores/climatologicos/diarios/datos/"
        "fechaini/{start}/fechafin/{end}/estacion/{station_code}"
    )
    start = date.strftime("%Y-%m-%dT00:00:00UTC")
    end = date.strftime("%Y-%m-%dT23:59:59UTC")
    endpoint = template.format(start=start, end=end, station_code=station_code)
    data_url = _get_data_url(endpoint, city_id, api_key, "METEO")
    if not data_url:
        return None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(data_url)
            if response.status_code in RETRY_STATUS_CODES:
                logging.warning(
                    f"METEO - City {city_id} - status {response.status_code}. "
                    f"Attempt {attempt}/{MAX_RETRIES}"
                )
                if attempt < MAX_RETRIES:
                    time.sleep(DELAY)
                    continue
            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            status = getattr(e.response, 'status_code', None)
            if status in RETRY_STATUS_CODES and attempt < MAX_RETRIES:
                logging.warning(
                    f"METEO - City {city_id} - request error {status or ''}: {e}. "
                    f"Attempt {attempt}/{MAX_RETRIES}"
                )
                time.sleep(DELAY)
                continue
            logging.error(f"METEO - City {city_id} - data URL fetch error: {e}")
            return None

    return None


def fetch_prediction_raw(city_id, postal_code, api_key):
    """
    Fetches raw hourly prediction JSON from AEMET for the next 24h.
    """
    endpoint = (
        f"https://opendata.aemet.es/opendata/api/"
        f"prediccion/especifica/municipio/horaria/{postal_code}"
    )
    data_url = _get_data_url(endpoint, city_id, api_key, "PREDICTION")
    if not data_url:
        return None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(data_url)
            if response.status_code in RETRY_STATUS_CODES:
                logging.warning(
                    f"PREDICTION - City {city_id} - status {response.status_code}. "
                    f"Attempt {attempt}/{MAX_RETRIES}"
                )
                if attempt < MAX_RETRIES:
                    time.sleep(DELAY)
                    continue
            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            status = getattr(e.response, 'status_code', None)
            if status in RETRY_STATUS_CODES and attempt < MAX_RETRIES:
                logging.warning(
                    f"PREDICTION - City {city_id} - request error {status or ''}: {e}. "
                    f"Attempt {attempt}/{MAX_RETRIES}"
                )
                time.sleep(DELAY)
                continue
            logging.error(f"PREDICTION - City {city_id} - request error final: {e}")
            return None

    return None