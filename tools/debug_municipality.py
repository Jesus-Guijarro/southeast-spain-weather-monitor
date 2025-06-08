"""
Usage::
    python -m tools.debug_municipality --municipality_id 23 [--days_ago 6]
"""
import argparse
import json
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv

from src.extract import get_observed_raw, get_forecast_raw
from src.transform import transform_observed, transform_forecast
from src.pipeline import get_connection

# Helper
def pretty_print(obj: object):
    """Pretty-print any JSON-serialisable object (2-space indent, UTF-8)."""
    return json.dumps(obj, indent=2, ensure_ascii=False)

def main():
    parser = argparse.ArgumentParser(
        description="Diagnose why a municipality/station is not returning data from AEMET."
    )
    parser.add_argument(
        "--municipality_id",
        type=int,
        required=True,
        help="Municipality identifier present in the 'municipalities' table.",
    )
    parser.add_argument(
        "--days_ago",
        type=int,
        default=6,
        help="Number of days back for historic station data (default: 6).",
    )
    args = parser.parse_args()

    # Look up the municipality in the database to obtain its postal and station codes
    conn, cur = get_connection()
    cur.execute(
        "SELECT postal_code, station_code FROM municipalities WHERE municipality_id = %s;",
        (args.municipality_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row is None:
        print(f"[ERROR] municipality_id {args.municipality_id} not found in table 'municipalities'.")
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
        f"\n--- Diagnosis for municipality_id={args.municipality_id} "
        f"(station={station_code}, postal={postal_code}) ---"
    )
    print(f"Target historic date: {target_date.strftime('%Y-%m-%d')}\n")

    # OBSERVED extraction
    print("=> Fetching OBSERVED …")
    raw_observed = get_observed_raw(args.municipality_id, station_code, target_date, api_key)
    if raw_observed is None:
        print("❌  OBSERVED fetch failed (check messages above).")
    else:
        print("✅  OBSERVED fetch succeeded.")
        print("Transformed record:")
        print(pretty_print(transform_observed(raw_observed, args.municipality_id, target_date)))

    # FORECAST extraction
    print("\n=> Fetching FORECAST …")
    raw_forecast = get_forecast_raw(args.municipality_id, postal_code, api_key)
    if raw_forecast is None:
        print("❌  FORECAST fetch failed (check messages above).")
    else:
        print("✅  FORECAST fetch succeeded.")
        print("Transformed record:")
        print(pretty_print(transform_forecast(raw_forecast, args.municipality_id)))

if __name__ == "__main__":
    main()
