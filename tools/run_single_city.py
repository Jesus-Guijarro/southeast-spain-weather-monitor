"""
Usage::

    python -m tools.run_single_city --city_id 23 [--days_ago 6]

Arguments::

--city_id  : (required) Identifier present in the *cities* table.
--days_ago : Historic offset for station observations (default: 6).

"""

import argparse
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv

from src.extract import fetch_meteo_raw, fetch_prediction_raw
from src.transform import transform_meteo, transform_prediction
from src.load import insert_meteo_data, insert_prediction_data
from src.pipeline import get_connection


# ---------------------------------------------------------------------------
# Main program
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the ETL pipeline for a single city."
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

    # Retrieve city metadata (postal & station codes)
    conn, cur = get_connection()
    cur.execute(
        "SELECT postal_code, station_code FROM cities WHERE city_id = %s;",
        (args.city_id,),
    )
    row = cur.fetchone()
    if row is None:
        print(f"[ERROR] city_id {args.city_id} not found in table 'cities'.")
        cur.close()
        conn.close()
        return

    postal_code, station_code = row
    target_date = datetime.now() - timedelta(days=args.days_ago)

    # Load API key
    load_dotenv()
    api_key = os.getenv("API_KEY_WEATHER")
    if not api_key:
        print("[ERROR] Environment variable API_KEY_WEATHER is not set.")
        cur.close()
        conn.close()
        return

    print(f"\n--- ETL for city_id={args.city_id} (station={station_code}, postal={postal_code}) ---")

    # Extract
    raw_met = fetch_meteo_raw(args.city_id, station_code, target_date, api_key)
    raw_pred = fetch_prediction_raw(args.city_id, postal_code, api_key)

    # Transform
    met = transform_meteo(raw_met, args.city_id, target_date)
    pred = transform_prediction(raw_pred, args.city_id)

    # Load
    if met or pred:
        if pred:
            insert_prediction_data(cur, pred)
        if met:
            insert_meteo_data(cur, met)
        conn.commit()
        print("✅  Data committed to database successfully.")
    else:
        print("❌  One or both transformations failed; nothing written to DB.")
        if not met:
            print("   - METEO data missing or invalid")
        if not pred:
            print("   - PREDICTION data missing or invalid")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
