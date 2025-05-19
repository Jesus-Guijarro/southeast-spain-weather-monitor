import json

def insert_meteo_data(cursor, data):
    """
    Insert meteo data in WEATHER_DATA table
    """
    # Prepare SQL statement to update with meteo data into an existing weather_data record
    q = """
    INSERT INTO weather_data (
      city_id,
      date,
      temperature_measured_avg,
      temperature_measured_max,
      temperature_measured_min,
      humidity_measured_avg,
      humidity_measured_max,
      humidity_measured_min,
      precipitation
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (city_id, date) DO UPDATE
      SET
        temperature_measured_avg = EXCLUDED.temperature_measured_avg,
        temperature_measured_max = EXCLUDED.temperature_measured_max,
        temperature_measured_min = EXCLUDED.temperature_measured_min,
        humidity_measured_avg = EXCLUDED.humidity_measured_avg,
        humidity_measured_max = EXCLUDED.humidity_measured_max,
        humidity_measured_min = EXCLUDED.humidity_measured_min,
        precipitation = EXCLUDED.precipitation;
    """
    cursor.execute(q, (
        data['city_id'],
        data['date'],
        data['temperature_avg'],
        data['temperature_max'],
        data['temperature_min'],
        data['humidity_avg'],
        data['humidity_max'],
        data['humidity_min'],
        data['precipitation']
    ))


def insert_prediction_data(cursor, data):
    """
    Insert prediction data into WEATHER_DATA table
    """
    # Prepare INSERT statement with ON CONFLICT to avoid duplicate entries
    q = """
    INSERT INTO weather_data (
      city_id,
      date,
      temperature_predicted_max,
      temperature_predicted_min,
      temperature_predicted_avg,
      humidity_predicted_avg,
      humidity_predicted_max,
      humidity_predicted_min,
      precipitations,
      prob_precipitation,
      prob_storm
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (city_id, date) DO UPDATE
      SET
        temperature_predicted_max = EXCLUDED.temperature_predicted_max,
        temperature_predicted_min = EXCLUDED.temperature_predicted_min,
        temperature_predicted_avg = EXCLUDED.temperature_predicted_avg,
        humidity_predicted_avg = EXCLUDED.humidity_predicted_avg,
        humidity_predicted_max = EXCLUDED.humidity_predicted_max,
        humidity_predicted_min = EXCLUDED.humidity_predicted_min,
        precipitations = EXCLUDED.precipitations,
        prob_precipitation = EXCLUDED.prob_precipitation,
        prob_storm = EXCLUDED.prob_storm;
    """
    cursor.execute(q, (
        data['city_id'],
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