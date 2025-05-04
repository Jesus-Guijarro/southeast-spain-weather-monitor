import json

def insert_meteo_data(cursor, data):
    """
    Insert meteo data in WEATHER_DATA table
    """
    # Prepare SQL statement to update with meteo data into an existing weather_data record
    q = """
    UPDATE weather_data
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

    # Execute the update with values extracted
    cursor.execute(q, (
        data['temperature_avg'], 
        data['temperature_max'], 
        data['temperature_min'],
        data['humidity_avg'], 
        data['humidity_max'], 
        data['humidity_min'],
        data['precipitation'], 
        data['city_id'], 
        data['date']
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
    ON CONFLICT (city_id, date) DO NOTHING;
    """
    # Serialize list or dict fields to JSON where necessary, then execute
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