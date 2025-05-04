import pytest
from datetime import datetime, date, timedelta
import transform

@pytest.mark.parametrize("input_value, expected", [
    (None, None),
    ('Ip', None),
    ('3,1415', 3.14),
    ('2.718', 2.72),
    (5.678, 5.68),
])
def test_to_float(input_value, expected):
    # _to_float should handle None, 'Ip', comma/punto decimal, and float inputs
    assert transform._to_float(input_value) == expected


@pytest.mark.parametrize("input_value, expected", [
    (None, None),
    ('Ip', None),
    ('42', 42),
    (7, 7),
])
def test_to_int(input_value, expected):
    # _to_int should convert numeric strings and ints, handle 'Ip' and None
    assert transform._to_int(input_value) == expected

def test_transform_meteo_none_or_empty():
    # Input cases with no valid data should return None
    assert transform.transform_meteo([], city_id=1, date=date.today()) is None
    assert transform.transform_meteo(None, city_id=1, date=date.today()) is None
    assert transform.transform_meteo({}, city_id=1, date=date.today()) is None

def test_transform_meteo_correct_values():
    # Verify correct parsing and conversion of various fields
    raw = [{
        'prec': '1,234',
        'tmed': '10.5',
        'tmax': None,
        'tmin': '0,0',
        'hrMedia': '80',
        'hrMax': '90',
        'hrMin': '70'
    }]
    d = date(2025, 5, 3)
    out = transform.transform_meteo(raw, city_id=99, date=d)
    assert out == {
        'city_id': 99,
        'date': '2025-05-03',
        'precipitation': 1.23,
        'temperature_avg': 10,
        'temperature_max': None,
        'temperature_min': 0,
        'humidity_avg': 80,
        'humidity_max': 90,
        'humidity_min': 70
    }

def test_transform_prediction_none():
    # Cases where prediction data is missing or empty should return None
    base = [{'prediccion': {'dia': []}}]
    assert transform.transform_prediction([], city_id=5) is None
    assert transform.transform_prediction(None, city_id=5) is None
    assert transform.transform_prediction([{}], city_id=5) is None
    assert transform.transform_prediction(base, city_id=5) is None

def test_transform_prediction_correct_values():
    # Validate parsing of nested prediction structure and correct aggregation
    tomorrow_data = {
        'prediccion': {
            'dia': [
                {},
                {
                    'temperatura': [{'value': '5'}, {'value': '15'}, {'value': '10'}],
                    'humedadRelativa': [{'value': '40'}, {'value': '60'}],
                    'precipitacion': [{'value': '2'}],
                    'probPrecipitacion': [{'value': '30'}],
                    'probTormenta': [{'value': '5'}],
                }
            ]
        }
    }
    out = transform.transform_prediction([tomorrow_data], city_id=123)
    expected_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    assert out['date'] == expected_date
    
    assert out['temperature_max'] == 15.0
    assert out['temperature_min'] == 5.0
    assert out['temperature_avg'] == 10

    assert out['humidity_max'] == 60.0
    assert out['humidity_min'] == 40.0
    assert out['humidity_avg'] == 50

    assert out['precipitations'] == [{'value': '2'}]
    assert out['prob_precipitation'] == [{'value': '30'}]
    assert out['prob_storm'] == [{'value': '5'}]

    assert out['city_id'] == 123
