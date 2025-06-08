"""
Usage
-----
    python -m tools.get_raw_json --municipality_id 23
"""
import argparse
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from src.extract import get_observed_raw, get_forecast_raw
from src.pipeline import get_connection

def main():
    parser = argparse.ArgumentParser(
        description="Fetch raw JSON responses for OBSERVED and FORECAST for a specified municipality and store them as files in the current working directory."
    )
    parser.add_argument(
        "--municipality_id",
        type=int,
        required=True,
        help="Municipality identifier present in the 'municipalities' table.",
    )
    args = parser.parse_args()

    # Output directory is the 'tools' package directory
    output_dir = Path(__file__).parent.resolve()

    # Load API key from .env file
    load_dotenv()
    api_key = os.getenv("API_KEY_WEATHER")
    if not api_key:
        print("[ERROR] Environment variable API_KEY_WEATHER is not set.")
        return

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
    date_tag = target_date.strftime("%Y-%m-%d")

    # Fetch OBSERVED data
    raw_observed = get_observed_raw(args.municipality_id, station_code, target_date, api_key)
    if raw_observed is None:
        print("❌  OBSERVED fetch failed (check messages above).")
    else:
        observed_filename = f"OBSERVED-{args.municipality_id}-{date_tag}.json"
        observed_path = output_dir / observed_filename
        with observed_path.open("w", encoding="utf-8") as f:
            json.dump(raw_observed, f, ensure_ascii=False, indent=2)
        print(f"✅  OBSERVED JSON guardado en tools/{observed_filename}")

    # Fetch FORECAST data
    raw_forecast = get_forecast_raw(args.municipality_id, postal_code, api_key)
    if raw_forecast is None:
        print("❌  FORECAST fetch failed (check messages above).")
    else:
        today_tag = datetime.now().strftime("%Y-%m-%d")
        forecast_filename = f"FORECAST-{args.municipality_id}-{today_tag}.json"
        forecast_path = output_dir / forecast_filename
        with forecast_path.open("w", encoding="utf-8") as f:
            json.dump(raw_forecast, f, ensure_ascii=False, indent=2)
        print(f"✅  FORECAST JSON guardado en tools/{forecast_filename}")


if __name__ == "__main__":
    main()
