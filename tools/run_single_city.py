"""
Usage
-----
# 1) Observed + Forecast data (default mode: observed uses offset -6 days)
python -m tools.run_single_municipality --municipality_id 23

# 2) Forecast data ONLY
python -m tools.run_single_municipality --municipality_id 23 --forecast

# 3) Observed data ONLY for a specific date, formatted as YYYY-MM-DD
python -m tools.run_single_municipality --municipality_id 23 --date 2025-05-30
"""

import argparse
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from src.extract import fetch_observed_raw, fetch_forecast_raw
from src.transform import transform_observed, transform_forecast
from src.load import insert_observed_data, insert_forecast_data
from src.pipeline import get_connection


def get_municipality(cur, municipality_id):
    """Fetch postal and station codes for a municipality."""
    cur.execute(
        "SELECT postal_code, station_code FROM municipalities WHERE municipality_id = %s;",
        (municipality_id,),
    )
    return cur.fetchone()

def main():
    parser = argparse.ArgumentParser(description="Run the ETL pipeline for a single municipality.")
    parser.add_argument("--municipality_id", type=int, required=True,
                        help="Municipality identifier present in the 'municipalities' table.")
    parser.add_argument("--date", dest="date", type=str,
                        help="Run ONLY observed for the given date (YYYY-MM-DD).")
    parser.add_argument("--forecast", dest="forecast_only", action="store_true",
                        help="Run ONLY forecast.")
    args, _ = parser.parse_known_args()

    if args.date and args.forecast_only:
        parser.error("Choose either --date or --forecast, not both.")

    if args.date:
        # Mode 3: only observed for a specific date
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            parser.error("--date must be in YYYY-MM-DD format.")
        run_observed, run_forecast = True, False
    elif args.forecast_only:
        # Mode 2: only forecast
        target_date = None
        run_observed, run_forecast = False, True
    else:
        # Mode 1: both observed and forecast, default to -6 days offset
        target_date = datetime.now() - timedelta(days=6)
        run_observed, run_forecast = True, True

    conn, cur = get_connection()
    municipality = get_municipality(cur, args.municipality_id)
    if municipality is None:
        print(f"[ERROR] municipality_id {args.municipality_id} not found in 'municipalities' table.")
        cur.close()
        conn.close()
        return

    postal_code, station_code = municipality

    load_dotenv()
    api_key = os.getenv("API_KEY_WEATHER")
    if not api_key:
        print("[ERROR] Environment variable API_KEY_WEATHER is not set.")
        cur.close()
        conn.close()
        return
    
    # ------------------------------ Extract -------------------------------- #
    raw_observed = raw_forecast = None
    if run_observed:
        raw_observed = fetch_observed_raw(
            args.municipality_id, station_code, target_date, api_key
        )
    if run_forecast:
        raw_forecast = fetch_forecast_raw(
            args.municipality_id, postal_code, api_key
        )

    # ------------------------------ Transform ------------------------------- #
    observed_data = forecast_data = None
    if run_observed and raw_observed:
        observed_data = transform_observed(raw_observed, args.municipality_id, target_date)
    if run_forecast and raw_forecast:
        forecast_data = transform_forecast(raw_forecast, args.municipality_id)

    # -------------------------------- Load ---------------------------------- #
    wrote_anything = False
    if forecast_data:
        insert_forecast_data(cur, forecast_data)
        wrote_anything = True
    if observed_data:
        insert_observed_data(cur, observed_data)
        wrote_anything = True

    if wrote_anything:
        conn.commit()
        print("✅ Data committed to database successfully.")
    else:
        print("❌ Nothing written to DB:")
        if run_observed and not observed_data:
            print("   • OBSERVED data missing or invalid")
        if run_forecast and not forecast_data:
            print("   • FORECAST data missing or invalid")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
