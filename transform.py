from datetime import datetime, timedelta


def _to_float(value):
    """
    Function to convert strings to floats
    """
    if value is None or value == 'Ip':
        return None
    v = float(value.replace(',', '.') if isinstance(value, str) else value)
    return round(v, 2)


def _to_int(value):
    """
    Function to convert strings to integers
    """
    if value is None or value == 'Ip':
        return None
    return int(value)


def transform_meteo(raw_json, city_id, date):
    """
    Transforms raw meteo JSON into a cleaned dict ready for loading.
    """
    if not raw_json or not isinstance(raw_json, list) or len(raw_json) == 0:
        return None
    d0 = raw_json[0]
    return {
        'city_id': city_id,
        'date': date.strftime('%Y-%m-%d'),
        'precipitation': _to_float(d0.get('prec')),
        'temperature_avg': _to_float(d0.get('tmed')),
        'temperature_max': _to_float(d0.get('tmax')),
        'temperature_min': _to_float(d0.get('tmin')),
        'humidity_avg': _to_int(d0.get('hrMedia')),
        'humidity_max': _to_int(d0.get('hrMax')),
        'humidity_min': _to_int(d0.get('hrMin'))
    }


def transform_prediction(raw_json, city_id):
    """
    Transforms raw prediction JSON into a cleaned dict ready for loading.
    """
    if not raw_json or not isinstance(raw_json, list) or len(raw_json) == 0:
        return None
    pred = raw_json[0].get('prediccion', {})
    dias = pred.get('dia', [])
    if len(dias) < 2:
        return None
    tomorrow = dias[1]
    temps = tomorrow.get('temperatura', []) or []
    hums = tomorrow.get('humedadRelativa', []) or []
    temp_vals = [float(t['value']) for t in temps]
    hum_vals = [float(h['value']) for h in hums]
    return {
        'city_id': city_id,
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
