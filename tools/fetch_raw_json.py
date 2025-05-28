"""
Usage::
    python -m tools.fetch_raw_json --city_id 23 [--days_ago 6]
"""
import argparse
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from src.extract import fetch_meteo_raw, fetch_prediction_raw
from src.pipeline import get_connection


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch raw JSON responses for METEO and PREDICTION for a specified city and store them as files in the current working directory."
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

    # Output directory is the 'tools' package directory
    output_dir = Path(__file__).parent.resolve()

    # Load API key from .env file
    load_dotenv()
    api_key = os.getenv("API_KEY_WEATHER")
    if not api_key:
        print("[ERROR] Environment variable API_KEY_WEATHER is not set.")
        return

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
    target_date = datetime.now() - timedelta(days=args.days_ago)
    date_tag = target_date.strftime("%Y-%m-%d")

    # Fetch METEO data
    raw_met = fetch_meteo_raw(args.city_id, station_code, target_date, api_key)
    if raw_met is None:
        print("❌  METEO fetch failed (check messages above).")
    else:
        meteo_filename = f"METEO-{args.city_id}-{date_tag}.json"
        meteo_path = output_dir / meteo_filename
        with meteo_path.open("w", encoding="utf-8") as f:
            json.dump(raw_met, f, ensure_ascii=False, indent=2)
        print(f"✅  METEO JSON guardado en tools/{meteo_filename}")

    # Fetch PREDICTION data
    raw_pred = fetch_prediction_raw(args.city_id, postal_code, api_key)
    if raw_pred is None:
        print("❌  PREDICTION fetch failed (check messages above).")
    else:
        today_tag = datetime.now().strftime("%Y-%m-%d")
        pred_filename = f"PREDICTION-{args.city_id}-{today_tag}.json"
        pred_path = output_dir / pred_filename
        with pred_path.open("w", encoding="utf-8") as f:
            json.dump(raw_pred, f, ensure_ascii=False, indent=2)
        print(f"✅  PREDICTION JSON guardado en tools/{pred_filename}")


if __name__ == "__main__":
    main()
