import json

def insert_observed_data(cursor, data):
    """
    Insert observed data in WEATHER_records table
    """
    # Prepare SQL statement to update with observed data into an existing "weather_records" record
    q = """
    INSERT INTO weather_records (
      municipality_id,
      date,
      temperature_observed_avg,
      temperature_observed_max,
      temperature_observed_min,
      humidity_observed_avg,
      humidity_observed_max,
      humidity_observed_min,
      precipitation
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (municipality_id, date) DO UPDATE
      SET
        temperature_observed_avg = EXCLUDED.temperature_observed_avg,
        temperature_observed_max = EXCLUDED.temperature_observed_max,
        temperature_observed_min = EXCLUDED.temperature_observed_min,
        humidity_observed_avg = EXCLUDED.humidity_observed_avg,
        humidity_observed_max = EXCLUDED.humidity_observed_max,
        humidity_observed_min = EXCLUDED.humidity_observed_min,
        precipitation = EXCLUDED.precipitation;
    """
    cursor.execute(q, (
        data['municipality_id'],
        data['date'],
        data['temperature_avg'],
        data['temperature_max'],
        data['temperature_min'],
        data['humidity_avg'],
        data['humidity_max'],
        data['humidity_min'],
        data['precipitation']
    ))


def insert_forecast_data(cursor, data):
    """
    Insert forecast data into weather_records table
    """
    # Prepare INSERT statement with ON CONFLICT to avoid duplicate entries
    q = """
    INSERT INTO weather_records (
      municipality_id,
      date,
      temperature_forecast_max,
      temperature_forecast_min,
      temperature_forecast_avg,
      humidity_forecast_avg,
      humidity_forecast_max,
      humidity_forecast_min,
      precipitations,
      prob_precipitation,
      prob_storm
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (municipality_id, date) DO UPDATE
      SET
        temperature_forecast_max = EXCLUDED.temperature_forecast_max,
        temperature_forecast_min = EXCLUDED.temperature_forecast_min,
        temperature_forecast_avg = EXCLUDED.temperature_forecast_avg,
        humidity_forecast_avg = EXCLUDED.humidity_forecast_avg,
        humidity_forecast_max = EXCLUDED.humidity_forecast_max,
        humidity_forecast_min = EXCLUDED.humidity_forecast_min,
        precipitations = EXCLUDED.precipitations,
        prob_precipitation = EXCLUDED.prob_precipitation,
        prob_storm = EXCLUDED.prob_storm;
    """
    cursor.execute(q, (
        data['municipality_id'],
        data['date'],
        data['temperature_max'],
        data['temperature_min'],
        data['temperature_avg'],
        data['humidity_avg'],
        data['humidity_max'],
        data['humidity_min'],
        json.dumps(data['precipitations']),
        json.dumps(data['prob_precipitation']),
        json.dumps(data['prob_storm'])
    ))