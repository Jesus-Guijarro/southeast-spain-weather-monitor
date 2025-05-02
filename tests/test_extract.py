import datetime
import requests
from extract import (
    _get_json_with_retry,
    _get_data_url,
    fetch_meteo_raw,
    fetch_prediction_raw
)

class DummyResponse:
    def __init__(self, status_code, json_data=None, raise_exc=False):
        self.status_code = status_code
        self._json_data = json_data or {}
        self._raise_exc = raise_exc

    def raise_for_status(self):
        if self.status_code >= 400 or self._raise_exc:
            http_err = requests.HTTPError(f"Status {self.status_code}")
            # attach a dummy response
            http_err.response = self
            raise http_err

    def json(self):
        return self._json_data

def test_get_json_with_retry_success(monkeypatch):
    # 200 OK response
    resp = DummyResponse(200, {"foo": "bar"})
    monkeypatch.setattr("extract.requests.get", lambda url, headers=None, params=None: resp)
    monkeypatch.setattr("extract.time.sleep", lambda s: None)
    result = _get_json_with_retry("http://example.com")
    assert result == {"foo": "bar"}

def test_get_json_with_retry_retries_then_success(monkeypatch):
    # First 503, then 200 OK
    calls = [DummyResponse(503), DummyResponse(200, {"ok": True})]
    def fake_get(url, headers=None, params=None):
        return calls.pop(0)
    monkeypatch.setattr("extract.requests.get", fake_get)
    monkeypatch.setattr("extract.time.sleep", lambda s: None)
    result = _get_json_with_retry("url", query_type="Q", city_id=1)
    assert result == {"ok": True}

def test_get_json_with_retry_exception_and_final_failure(monkeypatch):
    # First 500, then exception and final failure 
    def fake_get(url, headers=None, params=None):
        err = requests.RequestException("fail")
        err.response = DummyResponse(500)
        raise err
    monkeypatch.setattr("extract.requests.get", fake_get)
    monkeypatch.setattr("extract.time.sleep", lambda s: None)
    result = _get_json_with_retry("url")
    assert result is None

def test_get_data_url_success(monkeypatch):
    # _get_json_with_retry return a JSON with 'datos' key
    monkeypatch.setattr("extract._get_json_with_retry",
                        lambda endpoint_url, headers, params, query_type, city_id: {"datos": "http://data.url"})
    url = _get_data_url("endpoint", city_id=42, api_key="key", query_type="T")
    assert url == "http://data.url"

def test_get_data_url_missing_datos(monkeypatch):
    # No 'datos' in JSON response
    monkeypatch.setattr("extract._get_json_with_retry",
                        lambda *args, **kwargs: {"otro": "valor"})
    url = _get_data_url("endpoint", city_id=1, api_key="key", query_type="X")
    assert url is None

def test_fetch_meteo_raw_short_circuit(monkeypatch):
    # if _get_data_url return None, fetch_meteo_raw should return None
    monkeypatch.setattr("extract._get_data_url", lambda *a, **k: None)
    res = fetch_meteo_raw("c", "s", datetime.date.today(), "key")
    assert res is None

def test_fetch_meteo_raw_success(monkeypatch):
    # Successful fetch of meteo data
    monkeypatch.setattr("extract._get_data_url", lambda *a, **k: "http://d")
    monkeypatch.setattr("extract._get_json_with_retry", lambda url, **k: {"data": 123})
    hoy = datetime.date(2025, 5, 3)
    res = fetch_meteo_raw("cid", "st", hoy, "apikey")
    assert res == {"data": 123}

def test_fetch_prediction_raw_success(monkeypatch):
    # Successful fetch of prediction data
    monkeypatch.setattr("extract._get_data_url", lambda *a, **k: "http://pred")
    monkeypatch.setattr("extract._get_json_with_retry", lambda url, **k: {"hourly": []})
    res = fetch_prediction_raw("cid", "28079", "apikey")
    assert res == {"hourly": []}

def test_fetch_prediction_raw_short_circuit(monkeypatch):
    # If _get_data_url return None, fetch_prediction_raw should return None
    monkeypatch.setattr("extract._get_data_url", lambda *a, **k: None)
    res = fetch_prediction_raw("cid", "28079", "apikey")
    assert res is None
