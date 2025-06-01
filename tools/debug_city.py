"""
Usage::
    python -m tools.debug_city --city_id 23 [--days_ago 6]
"""
import argparse
import json
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv

from src.extract import fetch_meteo_raw, fetch_prediction_raw
from src.transform import transform_meteo, transform_prediction
from src.pipeline import get_connection

# Helper
def _pp(obj: object) -> str:
    """Pretty-print any JSON-serialisable object (2-space indent, UTF-8)."""
    return json.dumps(obj, indent=2, ensure_ascii=False)

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Diagnose why a city/station is not returning data from AEMET."
    )
    parser.add_argument(
        "--city_id",
        type=int,
        required=True,
        help="City identifier present in the 'cities' table.",
    )
    parser.add_argument(
        "--days_ago",
        type=int,
        default=6,
        help="Number of days back for historic station data (default: 6).",
    )
    args = parser.parse_args()

    
    # Look up the city in the database to obtain its postal and station codes
    conn, cur = get_connection()
    cur.execute(
        "SELECT postal_code, station_code FROM cities WHERE city_id = %s;",
        (args.city_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row is None:
        print(f"[ERROR] city_id {args.city_id} not found in table 'cities'.")
        return

    postal_code, station_code = row
    target_date = datetime.now() - timedelta(days=6)

    # Load API key from .env file
    load_dotenv()
    api_key = os.getenv("API_KEY_WEATHER")
    if not api_key:
        print("[ERROR] Environment variable API_KEY_WEATHER is not set.")
        return

    print(
        f"\n--- Diagnosis for city_id={args.city_id} "
        f"(station={station_code}, postal={postal_code}) ---"
    )
    print(f"Target historic date: {target_date.strftime('%Y-%m-%d')}\n")

    # METEO extraction
    print("=> Fetching METEO …")
    raw_met = fetch_meteo_raw(args.city_id, station_code, target_date, api_key)
    if raw_met is None:
        print("❌  METEO fetch failed (check messages above).")
    else:
        print("✅  METEO fetch succeeded.")
        print("Transformed record:")
        print(_pp(transform_meteo(raw_met, args.city_id, target_date)))

    # PREDICTION extraction
    print("\n=> Fetching PREDICTION …")
    raw_pred = fetch_prediction_raw(args.city_id, postal_code, api_key)
    if raw_pred is None:
        print("❌  PREDICTION fetch failed (check messages above).")
    else:
        print("✅  PREDICTION fetch succeeded.")
        print("Transformed record:")
        print(_pp(transform_prediction(raw_pred, args.city_id)))


if __name__ == "__main__":
    main()
