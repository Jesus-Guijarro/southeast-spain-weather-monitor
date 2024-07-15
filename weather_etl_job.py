import json
import os
from datetime import datetime, timedelta
import logging

import database.database as db

# Get today's date and the date 5 days before
today = datetime.now().date()
days_before = today - timedelta(days=5)

base_path = f"data/{days_before.strftime('%d-%m-%Y')}"
#base_path = f"data/10-07-2024"

try:
    # Establish database connection
    conn, cursor = db.get_connection()

     # Walk through the directory to find all files
    for root, dirs, files in os.walk(base_path):
        files.sort()
        for filename in files:
            file_path = os.path.join(root, filename)

            # Extract filename and split into parts
            filename = os.path.basename(file_path)
            filename = filename.replace('.json', '')
            parts = filename.split('-')

            # Get "city_code" and "date" from the filename
            city_code = parts[0]
            date = '-'.join(parts[-3:])

            # Open and load JSON data
            with open(file_path, 'r') as file:
                data = json.load(file)

            if "meteo" in parts:
                # Process meteorological data
                data= data[0]
                
                # Get min, max and avg temperature values
                min_temperature = round(float(data["tmin"].replace(",", ".")))
                max_temperature = round(float(data["tmax"].replace(",", ".")))
                avg_temperature = round(float(data["tmed"].replace(",", ".")))

                # Insert data into WEATHER_DATA table
                insert_query = """
                INSERT INTO WEATHER_DATA (
                    city_code, date, temperature_measured_avg, temperature_measured_max, temperature_measured_min
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (city_code, date) DO NOTHING;
                """
                cursor.execute(insert_query, (city_code, date, avg_temperature, max_temperature, min_temperature))

                # Commit the transaction
                conn.commit()

            else:
                # Get temperature values from json file
                temperatures = data[0]['prediccion']['dia'][1]['temperatura']
                values = []

                 # Extract and convert temperature values to float
                for temp in temperatures:
                    values.append(float(temp['value']))

                # Calculate max, min, and average temperatures
                max_temperature = max(values)
                min_temperature = min(values)
                avg_temperature = round(float(sum(values)/len(values)))

                # Update data in WEATHER_DATA table
                update_query = """
                    UPDATE WEATHER_DATA
                    SET temperature_predicted_avg = %s,
                        temperature_predicted_max = %s,
                        temperature_predicted_min = %s
                    WHERE city_code = %s
                    AND date = %s;
                    """
                cursor.execute(update_query, (avg_temperature, max_temperature, min_temperature, city_code, date))

                # Commit the transaction
                conn.commit()

    # Close the database connection
    cursor.close()
    conn.close()

except Exception as e:
    # Log any errors that occur during the process
    logging.error(f"Error insert data in db. Day: {days_before}")