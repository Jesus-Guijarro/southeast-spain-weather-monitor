from datetime import datetime, timedelta


def to_float(value):
    """
    Function to convert strings to floats
    """
    # Handle missing values or placeholders
    if value is None or value == 'Ip':
        return None
    
    # Replace comma with dot for European decimal format, then cast to float
    v = float(value.replace(',', '.') if isinstance(value, str) else value)

    # Round to 2 decimal places
    return round(v, 2)


def to_int(value):
    """
    Function to convert strings to integers
    """
    if value is None or value == 'Ip':
        return None
    return int(value)


def transform_observed(raw_json, municipality_id, date):
    """
    Clean and reshape daily observed observations into a flat dict.

    Parameters:
        raw_json (list of dict): List containing one dict of daily stats.
        municipality_id: Identifier for the municipality.
        date (datetime): Date for which data was fetched.

    Returns:
        dict with selected metrics (rounded where appropriate), or None if input invalid.
    """
    # Validate input: expect a non-empty list
    if not raw_json or not isinstance(raw_json, list) or len(raw_json) == 0:
        return None
    
    observed_data = raw_json[0]
    
    return {
        'municipality_id': municipality_id,
        'date': date.strftime('%Y-%m-%d'),
        'precipitation': to_float(observed_data.get('prec')),
        'temperature_avg': round(to_float(observed_data.get('tmed'))) if to_float(observed_data.get('tmed')) is not None else None,
        'temperature_max': round(to_float(observed_data.get('tmax'))) if to_float(observed_data.get('tmax')) is not None else None,
        'temperature_min': round(to_float(observed_data.get('tmin'))) if to_float(observed_data.get('tmin')) is not None else None,
        'humidity_avg': to_int(observed_data.get('hrMedia')),
        'humidity_max': to_int(observed_data.get('hrMax')),
        'humidity_min': to_int(observed_data.get('hrMin'))
    }


def transform_forecast(raw_json, municipality_id):
    """
    Clean and reshape hourly forecast for the next day into summary metrics.

    Parameters:
        raw_json (list of dict): AEMET forecast response, with 'prediccion' key.
        municipality_id: Identifier for the municipality.

    Returns:
        dict summarizing tomorrow's temperature and humidity extremes and averages,
        plus precipitation and storm probabilities, or None on invalid input.
    """
    # Basic validation: need at least one element with 'prediccion'
    if not raw_json or not isinstance(raw_json, list) or len(raw_json) == 0:
        return None
    forecast = raw_json[0].get('prediccion', {})
    days = forecast.get('dia', []) # List of day-wise forecasts

    # Expect today's (0) and tomorrow's (1) entries
    if len(days) < 2:
        return None
    
    tomorrow = days[1]

    # Extract lists of hourly temperature and humidity readings
    temps = tomorrow.get('temperatura', []) or []
    hums = tomorrow.get('humedadRelativa', []) or []

    # Convert string values to floats for aggregation
    temp_vals = [float(t['value']) for t in temps]
    hum_vals = [float(h['value']) for h in hums]
    
    return {
        'municipality_id': municipality_id,
        'date': (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d'),
        'temperature_max': max(temp_vals) if temp_vals else None,
        'temperature_min': min(temp_vals) if temp_vals else None,
        'temperature_avg': round(sum(temp_vals)/len(temp_vals)) if temp_vals else None,
        'humidity_max': max(hum_vals) if hum_vals else None,
        'humidity_min': min(hum_vals) if hum_vals else None,
        'humidity_avg': round(sum(hum_vals)/len(hum_vals)) if hum_vals else None,
        'precipitations': tomorrow.get('precipitacion'),
        'prob_precipitation': tomorrow.get('probPrecipitacion'),
        'prob_storm': tomorrow.get('probTormenta')
    }
