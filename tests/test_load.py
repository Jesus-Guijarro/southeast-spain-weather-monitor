import json
from unittest.mock import Mock
import load

def test_insert_observed_data_calls_execute_with_correct_parameters():
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
        'municipality_id': 123,
        'date': '2025-05-03'
    }

    # Call the function under test
    load.insert_observed_data(cursor, data)

    # Expected SQL with placeholders matching order of params
    expected_query = """
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

    executed_query, params = cursor.execute.call_args[0]

    # Compare stripped queries to avoid whitespace differences
    assert expected_query.strip() == executed_query.strip()  

def test_insert_forecast_data_calls_execute_with_correct_parameters():
    # Prepare fake cursor and sample forecast data
    cursor = Mock()
    data = {
        'municipality_id': 456,
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
    load.insert_forecast_data(cursor, data)

    expected_query = """
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

    executed_query, params = cursor.execute.call_args[0]
    
    # Compare stripped queries to avoid whitespace differences
    assert expected_query.strip() == executed_query.strip() 

    # Ensure JSON fields are serialized correctly
    assert params == (
        data['municipality_id'], data['date'],
        data['temperature_max'], data['temperature_min'], data['temperature_avg'],
        data['humidity_avg'], data['humidity_max'], data['humidity_min'],
        json.dumps(data['precipitations']),
        json.dumps(data['prob_precipitation']),
        json.dumps(data['prob_storm'])
    )
