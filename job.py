import json
import os
from datetime import datetime, timedelta
import logging

import database as db

today = datetime.now().date()
days_before = today - timedelta(days=5)

base_path = f"data/{days_before.strftime('%d-%m-%Y')}"

try:
    conn, cursor = db.get_connection()

    for root, dirs, files in os.walk(base_path):
        for filename in files:
            file_path = os.path.join(root, filename)

            filename = os.path.basename(file_path)
            filename = filename.replace('.json', '')
            parts = filename.split('-')

            city_code = parts[0]
            fecha = '-'.join(parts[-3:])

            with open(file_path, 'r') as file:
                data = json.load(file)

            if "meteo" in parts:

                data= data[0]
                
                tmin = float(data["tmin"].replace(",", "."))
                tmax = float(data["tmax"].replace(",", "."))
                tavg = float(data["tmed"].replace(",", "."))

                insert_query = """
                INSERT INTO WEATHER_DATA (
                    city_code, date, temperature_measured_avg, temperature_measured_max, temperature_measured_min
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (city_code, date) DO NOTHING;
                """
                cursor.execute(insert_query, (city_code, fecha, tavg, tmax, tmin))

                conn.commit()

            else:
                temperatures = data[0]['prediccion']['dia'][1]['temperatura']
                values = []

                for temp in temperatures:
                    values.append(float(temp['value']))

                tmax=max(values)
                tmin=min(values)
                tavg = (sum(values)/len(values))

                update_query = """
                    UPDATE WEATHER_DATA
                    SET temperature_predicted_avg = %s,
                        temperature_predicted_max = %s,
                        temperature_predicted_min = %s
                    WHERE city_code = %s
                    AND date = %s;
                    """
                cursor.execute(update_query, (tavg, tmax, tmin, city_code, fecha))

                conn.commit()
    cursor.close()
    conn.close()

except Exception as e:
    logging.error(f"Error insert data in db. Day: {days_before}")