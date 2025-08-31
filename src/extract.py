import requests
import logging
import time

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# HTTP status codes that should trigger a retry
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
# Maximum number of retry attempts
MAX_RETRIES = 5
# Delay between retries in seconds
DELAY = 5

def get_wait_time(response_status, response_headers, delay=DELAY):
    """
    Calculate how long to wait before retrying.
    Handles 'Retry-After' header if status is 429.
    """
    wait_time = delay
    if response_status == 429 and response_headers:
        retry_after = response_headers.get("Retry-After")
        if retry_after:
            try:
                wait_time = int(retry_after)
            except ValueError:
                try:
                    retry_date = parsedate_to_datetime(retry_after)
                    now = datetime.now(timezone.utc)
                    wait_time = max((retry_date - now).total_seconds(), delay)
                except Exception:
                    wait_time = delay
    return wait_time

def get_json_with_retry(url, headers=None, params=None, query_type=None, municipality_id=None):
    """
    Perform an HTTP GET with automatic retries on transient failures.

    Args:
        url (str): Full endpoint URL to request.
        headers (dict, optional): HTTP headers to include.
        params (dict, optional): Query parameters for the request.
        query_type (str, optional): Label for logging context (e.g., 'OBSERVED').
        municipality_id (int or str, optional): Identifier for the municipality, for logging.

    Returns:
        dict or list: Parsed JSON response on success, or None on failure.
    """
    # Try up to MAX_RETRIES times
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=(5, 10)) # 5s connect, 10s read
            status = response.status_code

            # If we get a retryable status code, log and retry
            if status in RETRY_STATUS_CODES:
                logging.warning(
                    f"{query_type} - municipality {municipality_id} - status {status}. "
                    f"Attempt {attempt}/{MAX_RETRIES}"
                )
                wait_time = get_wait_time(status, getattr(response, 'headers', None))
                time.sleep(wait_time)
                continue

            # Raise for non-2xx responses not explicitly retried above    
            response.raise_for_status()
            return response.json() # return parsed JSON if successful

        except requests.RequestException as e:
            # Attempt to extract HTTP status if available
            status = getattr(e.response, 'status_code', None)

            # Only retry on configured status codes or network errors
            if (status in RETRY_STATUS_CODES or status is None):
                logging.warning(
                    f"{query_type} - Municipality {municipality_id} - request error {status or ''}: {e}. "
                    f"Attempt {attempt}/{MAX_RETRIES}"
                )
                status = getattr(e.response, 'status_code', None)
                headers = getattr(e.response, 'headers', None)
                wait_time = get_wait_time(status, headers)
                time.sleep(wait_time)
                continue

            # Log final failure if out of retries or unrecoverable error
            logging.error(f"{query_type} - Municipality {municipality_id} - request error final: {e}")
            return None
    # Exhausted all retries without success
    return None

def get_data_url(endpoint_url, municipality_id, api_key, query_type):
    """
    Retrieve the actual data URL ('datos') from the AEMET API response.

    Many AEMET endpoints return a JSON containing a 'datos' key,
    which is a URL pointing to the real data payload.
    """
    # Standard headers and API key parameter for the initial metadata call
    headers = {"cache-control": "no-cache", "accept": "application/json"}
    params = {"api_key": api_key}

    # Fetch the metadata JSON that includes 'datos'
    data = get_json_with_retry(
        endpoint_url, headers=headers, params=params,
        query_type=query_type, municipality_id=municipality_id
    )

    # Ensure the response contains the 'datos' field
    if not data or 'datos' not in data:
        logging.error(f"{query_type} - Municipality {municipality_id} - 'datos' key missing or fetch failed")
        return None
    
    # Return the URL where the actual JSON data resides
    return data['datos']

def get_observed_raw(municipality_id, station_code, date, api_key):
    """
    Fetch raw daily observed observations for a station on a given date.

    This function:
      1. Constructs the AEMET API endpoint for daily station data.
      2. Retrieves the 'datos' link via get_data_url.
      3. Fetches and returns the actual JSON readings.
    """

    # Format the start and end timestamps for the full day in UTC
    start = date.strftime("%Y-%m-%dT00:00:00UTC")
    end = date.strftime("%Y-%m-%dT23:59:59UTC")

    url = f"https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start}/fechafin/{end}/estacion/{station_code}"

    # First call to retrieve the data URL
    data_url = get_data_url(url, municipality_id, api_key, "OBSERVED")
    if not data_url:
        return None
    
    # Second call to fetch the actual observed data
    return get_json_with_retry(data_url, query_type="OBSERVED", municipality_id=municipality_id)

def get_forecast_raw(municipality_id, postal_code, api_key):
    """
    Fetch raw hourly weather forecast for the next 24 hours for a municipality.

    Steps:
      1. Call the AEMET 'prediccion' endpoint for the given postal code.
      2. Extract the 'datos' URL via get_data_url.
      3. Retrieve and return the JSON forecast array.
    """
    # Construct the specific forecast endpoint URL for the municipality
    url = f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{postal_code}"
    

    # Obtain the URL where the 24h forecast JSON is hosted
    data_url = get_data_url(url, municipality_id, api_key, "FORECAST")
    if not data_url:
        return None
    
    # Fetch and return the actual forecast data
    return get_json_with_retry(data_url, query_type="FORECAST", municipality_id=municipality_id)
