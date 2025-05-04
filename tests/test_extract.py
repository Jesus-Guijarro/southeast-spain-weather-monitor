import datetime
import requests
from extract import (
    _get_json_with_retry,
    _get_data_url,
    fetch_meteo_raw,
    fetch_prediction_raw
)

class DummyResponse:
    """
    A fake response object to simulate requests.Response with controlled status and JSON data.
    """
    def __init__(self, status_code, json_data=None, raise_exc=False):
        self.status_code = status_code
        self._json_data = json_data or {}
        self._raise_exc = raise_exc # Flag to trigger an exception in raise_for_status

    def raise_for_status(self):
        # Emulate HTTPError for status codes >=400 or if raise_exc=True
        if self.status_code >= 400 or self._raise_exc:
            http_err = requests.HTTPError(f"Status {self.status_code}")
            http_err.response = self # Attach this DummyResponse to the exception
            raise http_err

    def json(self):
        # Return the predefined JSON payload
        return self._json_data

def test_get_json_with_retry_success(monkeypatch):
    # Simulate a successful 200 OK response on first try
    resp = DummyResponse(200, {"foo": "bar"})
    monkeypatch.setattr("extract.requests.get", lambda url, headers=None, params=None: resp)
    result = _get_json_with_retry("http://example.com")
    assert result == {"foo": "bar"}

def test_get_json_with_retry_retries_then_success(monkeypatch):
    # First a 503, then a 200 OK; ensure retry logic returns the final JSON
    calls = [DummyResponse(503), DummyResponse(200, {"ok": True})]
    def fake_get(url, headers=None, params=None):
        return calls.pop(0)
    monkeypatch.setattr("extract.requests.get", fake_get)
    monkeypatch.setattr("extract.time.sleep", lambda s: None)
    result = _get_json_with_retry("url", query_type="Q", city_id=1)
    assert result == {"ok": True}

def test_get_json_with_retry_exception_and_final_failure(monkeypatch):
    # Simulate request exception then failure; ensure function returns None on repeated errors
    def fake_get(url, headers=None, params=None):
        err = requests.RequestException("fail")
        err.response = DummyResponse(500)
        raise err
    monkeypatch.setattr("extract.requests.get", fake_get)
    monkeypatch.setattr("extract.time.sleep", lambda s: None)
    result = _get_json_with_retry("url")
    assert result is None

def test_get_data_url_success(monkeypatch):
   # If _get_json_with_retry returns a dict with 'datos', _get_data_url should extract it
    monkeypatch.setattr("extract._get_json_with_retry",
                        lambda endpoint_url, headers, params, query_type, city_id: {"datos": "http://data.url"})
    url = _get_data_url("endpoint", city_id=42, api_key="key", query_type="T")
    assert url == "http://data.url"

def test_get_data_url_missing_datos(monkeypatch):
    # If 'datos' key is missing, function should return None
    monkeypatch.setattr("extract._get_json_with_retry",
                        lambda *args, **kwargs: {"otro": "valor"})
    url = _get_data_url("endpoint", city_id=1, api_key="key", query_type="X")
    assert url is None

def test_fetch_meteo_raw_short_circuit(monkeypatch):
    # If _get_data_url returns None, fetch_meteo_raw should short-circuit and return None
    monkeypatch.setattr("extract._get_data_url", lambda *a, **k: None)
    res = fetch_meteo_raw("c", "s", datetime.date.today(), "key")
    assert res is None

def test_fetch_meteo_raw_success(monkeypatch):
    # Successful meteorological data fetch returns the JSON payload
    monkeypatch.setattr("extract._get_data_url", lambda *a, **k: "http://d")
    monkeypatch.setattr("extract._get_json_with_retry", lambda url, **k: {"data": 123})
    hoy = datetime.date(2025, 5, 3)
    res = fetch_meteo_raw("cid", "st", hoy, "apikey")
    assert res == {"data": 123}

def test_fetch_prediction_raw_success(monkeypatch):
    # Successful prediction fetch returns the hourly data
    monkeypatch.setattr("extract._get_data_url", lambda *a, **k: "http://pred")
    monkeypatch.setattr("extract._get_json_with_retry", lambda url, **k: {"hourly": []})
    res = fetch_prediction_raw("cid", "28079", "apikey")
    assert res == {"hourly": []}

def test_fetch_prediction_raw_short_circuit(monkeypatch):
    # If no URL is returned, prediction fetch should return None
    monkeypatch.setattr("extract._get_data_url", lambda *a, **k: None)
    res = fetch_prediction_raw("cid", "28079", "apikey")
    assert res is None
