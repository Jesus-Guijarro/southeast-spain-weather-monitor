import requests
import logging
import time

# HTTP status codes that should trigger a retry
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
# Maximum number of retry attempts
MAX_RETRIES = 5
# Delay between retries in seconds
DELAY = 10

def _get_json_with_retry(url, headers=None, params=None, query_type=None, city_id=None):
    """
    Perform an HTTP GET with automatic retries on transient failures.

    Args:
        url (str): Full endpoint URL to request.
        headers (dict, optional): HTTP headers to include.
        params (dict, optional): Query parameters for the request.
        query_type (str, optional): Label for logging context (e.g., 'METEO').
        city_id (int or str, optional): Identifier for the city, for logging.

    Returns:
        dict or list: Parsed JSON response on success, or None on failure.
    """
    # Try up to MAX_RETRIES times
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, params=params)
            status = response.status_code

            # If we get a retryable status code, log and retry
            if status in RETRY_STATUS_CODES:
                logging.warning(
                    f"{query_type} - City {city_id} - status {status}. "
                    f"Attempt {attempt}/{MAX_RETRIES}"
                )
                if attempt < MAX_RETRIES:
                    time.sleep(DELAY) 
                    continue

            # Raise for non-2xx responses not explicitly retried above    
            response.raise_for_status()
            return response.json() # return parsed JSON if successful

        except requests.RequestException as e:
            # Attempt to extract HTTP status if available
            status = getattr(e.response, 'status_code', None)

            # Only retry on configured status codes or network errors
            if (status in RETRY_STATUS_CODES or status is None) and attempt < MAX_RETRIES:
                logging.warning(
                    f"{query_type} - City {city_id} - request error {status or ''}: {e}. "
                    f"Attempt {attempt}/{MAX_RETRIES}"
                )
                time.sleep(DELAY)
                continue

            # Log final failure if out of retries or unrecoverable error
            logging.error(f"{query_type} - City {city_id} - request error final: {e}")
            return None
    # Exhausted all retries without success
    return None


def _get_data_url(endpoint_url, city_id, api_key, query_type):
    """
    Retrieve the actual data URL ('datos') from the AEMET API response.

    Many AEMET endpoints return a JSON containing a 'datos' key,
    which is a URL pointing to the real data payload.
    """
    # Standard headers and API key parameter for the initial metadata call
    headers = {"cache-control": "no-cache", "accept": "application/json"}
    params = {"api_key": api_key}

    # Fetch the metadata JSON that includes 'datos'
    data = _get_json_with_retry(
        endpoint_url, headers=headers, params=params,
        query_type=query_type, city_id=city_id
    )

    # Ensure the response contains the 'datos' field
    if not data or 'datos' not in data:
        logging.error(f"{query_type} - City {city_id} - 'datos' key missing or fetch failed")
        return None
    
    # Return the URL where the actual JSON data resides
    return data['datos']


def fetch_meteo_raw(city_id, station_code, date, api_key):
    """
    Fetch raw daily meteorological observations for a station on a given date.

    This function:
      1. Constructs the AEMET API endpoint for daily station data.
      2. Retrieves the 'datos' link via _get_data_url.
      3. Fetches and returns the actual JSON readings.
    """
    # AEMET API template with placeholders for start/end timestamps and station
    template = (
        "https://opendata.aemet.es/opendata/api/"
        "valores/climatologicos/diarios/datos/"
        "fechaini/{start}/fechafin/{end}/estacion/{station_code}"
    )

    # Format the start and end timestamps for the full day in UTC
    start = date.strftime("%Y-%m-%dT00:00:00UTC")
    end = date.strftime("%Y-%m-%dT23:59:59UTC")

    endpoint = template.format(
        start=start, 
        end=end, 
        station_code=station_code
    )

    # First call to retrieve the data URL
    data_url = _get_data_url(endpoint, city_id, api_key, "METEO")
    if not data_url:
        return None
    
    # Second call to fetch the actual meteo data
    return _get_json_with_retry(data_url, query_type="METEO", city_id=city_id)


def fetch_prediction_raw(city_id, postal_code, api_key):
    """
    Fetch raw hourly weather predictions for the next 24 hours for a municipality.

    Steps:
      1. Call the AEMET 'prediccion' endpoint for the given postal code.
      2. Extract the 'datos' URL via _get_data_url.
      3. Retrieve and return the JSON forecast array.
    """
    # Construct the specific prediction endpoint URL for the municipality
    endpoint = (
        f"https://opendata.aemet.es/opendata/api/"
        f"prediccion/especifica/municipio/horaria/{postal_code}"
    )

    # Obtain the URL where the 24h forecast JSON is hosted
    data_url = _get_data_url(endpoint, city_id, api_key, "PREDICTION")
    if not data_url:
        return None
    
    # Fetch and return the actual prediction data
    return _get_json_with_retry(data_url, query_type="PREDICTION", city_id=city_id)
