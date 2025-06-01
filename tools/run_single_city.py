"""
Usage
-----
# 1) Meteo + Prediction (default mode: meteo uses offset -6 days)
python -m tools.run_single_city --city_id 23

# 2) Prediction ONLY
python -m tools.run_single_city --city_id 23 --pred

# 3) Meteo ONLY for a specific date, formatted as YYYY-MM-DD
python -m tools.run_single_city --city_id 23 --met-date 2025-05-30
"""

import argparse
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from src.extract import fetch_meteo_raw, fetch_prediction_raw
from src.transform import transform_meteo, transform_prediction
from src.load import insert_meteo_data, insert_prediction_data
from src.pipeline import get_connection


def get_city(cur, city_id):
    """Fetch postal and station codes for a city."""
    cur.execute(
        "SELECT postal_code, station_code FROM cities WHERE city_id = %s;",
        (city_id,),
    )
    return cur.fetchone()


def main():
    parser = argparse.ArgumentParser(description="Run the ETL pipeline for a single city.")
    parser.add_argument("--city_id", type=int, required=True,
                        help="City identifier present in the 'cities' table.")
    parser.add_argument("--met-date", dest="met_date", type=str,
                        help="Run ONLY meteo for the given date (YYYY-MM-DD).")
    parser.add_argument("--pred", dest="pred_only", action="store_true",
                        help="Run ONLY prediction.")
    args, _ = parser.parse_known_args()

    if args.met_date and args.pred_only:
        parser.error("Choose either --met-date or --pred, not both.")

    if args.met_date:
        # Mode 3: only meteo for a specific date
        try:
            target_date = datetime.strptime(args.met_date, "%Y-%m-%d")
        except ValueError:
            parser.error("--met-date must be in YYYY-MM-DD format.")
        run_meteo, run_pred = True, False
    elif args.pred_only:
        # Mode 2: only prediction
        target_date = None
        run_meteo, run_pred = False, True
    else:
        # Mode 1: both meteo and prediction, default to -6 days offset
        target_date = datetime.now() - timedelta(days=6)
        run_meteo, run_pred = True, True

    conn, cur = get_connection()
    meta = get_city(cur, args.city_id)
    if meta is None:
        print(f"[ERROR] city_id {args.city_id} not found in 'cities' table.")
        cur.close()
        conn.close()
        return

    postal_code, station_code = meta


    load_dotenv()
    api_key = os.getenv("API_KEY_WEATHER")
    if not api_key:
        print("[ERROR] Environment variable API_KEY_WEATHER is not set.")
        cur.close()
        conn.close()
        return
    
    # ------------------------------ Extract -------------------------------- #
    raw_meteo = raw_pred = None
    if run_meteo:
        raw_meteo = fetch_meteo_raw(
            args.city_id, station_code, target_date, api_key
        )
    if run_pred:
        raw_pred = fetch_prediction_raw(
            args.city_id, postal_code, api_key
        )

    # ------------------------------ Transform ------------------------------- #
    meteo_data = pred_data = None
    if run_meteo and raw_meteo:
        meteo_data = transform_meteo(raw_meteo, args.city_id, target_date)
    if run_pred and raw_pred:
        pred_data = transform_prediction(raw_pred, args.city_id)

    # -------------------------------- Load ---------------------------------- #
    wrote_anything = False
    if pred_data:
        insert_prediction_data(cur, pred_data)
        wrote_anything = True
    if meteo_data:
        insert_meteo_data(cur, meteo_data)
        wrote_anything = True

    if wrote_anything:
        conn.commit()
        print("✅ Data committed to database successfully.")
    else:
        print("❌ Nothing written to DB:")
        if run_meteo and not meteo_data:
            print("   • METEO data missing or invalid")
        if run_pred and not pred_data:
            print("   • PREDICTION data missing or invalid")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
