import json
from unittest.mock import Mock
import load

def test_insert_meteo_data_calls_execute_with_correct_parameters():
    # Prepare a fake cursor and sample data dict
    cursor = Mock()
    data = {
        'temperature_avg': 20.5,
        'temperature_max': 25.0,
        'temperature_min': 15.0,
        'humidity_avg': 60.0,
        'humidity_max': 80.0,
        'humidity_min': 40.0,
        'precipitation': 5.0,
        'city_id': 123,
        'date': '2025-05-03'
    }

    # Call the function under test
    load.insert_meteo_data(cursor, data)

    # Expected SQL with placeholders matching order of params
    expected_query = """
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
    # Compare stripped queries to avoid whitespace differences
    executed_query, params = cursor.execute.call_args[0]
    assert expected_query.strip() == executed_query.strip()
    assert params == (
        data['temperature_avg'], data['temperature_max'], data['temperature_min'],
        data['humidity_avg'], data['humidity_max'], data['humidity_min'],
        data['precipitation'], data['city_id'], data['date']
    )


def test_insert_prediction_data_calls_execute_with_correct_parameters():
    # Prepare fake cursor and sample prediction data
    cursor = Mock()
    data = {
        'city_id': 456,
        'date': '2025-05-04',
        'temperature_max': 30.0,
        'temperature_min': 20.0,
        'temperature_avg': 25.0,
        'humidity_avg': 55.0,
        'humidity_max': 70.0,
        'humidity_min': 40.0,
        'precipitations': [0.1, 0.2],
        'prob_precipitation': {'rain': 0.3},
        'prob_storm': {'storm': 0.1}
    }

    # Call the function under test
    load.insert_prediction_data(cursor, data)

    expected_query = """
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

    executed_query, params = cursor.execute.call_args[0]
    assert expected_query.strip() == executed_query.strip() # Compare stripped queries to avoid whitespace differences

    # Ensure JSON fields are serialized correctly
    assert params == (
        data['city_id'], data['date'],
        data['temperature_max'], data['temperature_min'], data['temperature_avg'],
        data['humidity_avg'], data['humidity_max'], data['humidity_min'],
        json.dumps(data['precipitations']),
        json.dumps(data['prob_precipitation']),
        json.dumps(data['prob_storm'])
    )
