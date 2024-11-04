# Weather Spain Pipeline
## Introduction

This project is dedicated to monitoring meteorological data in the southeastern region of Spain, specifically in the provinces of Almería, Murcia, Alicante, and Valencia, areas identified as at-risk for desertification and prone to DANA events. Leveraging data provided by the AEMET (Agencia Estatal de Meteorología) API, this project aims to gather, process, and analyze weather information crucial for understanding these environmental risks.

## Objectives
- **Data Collection**: utilize the AEMET API to gather comprehensive meteorological data.
- **Analysis**: perform calculations and analysis on gathered data to assess climatic trends and risks.
- **Database Integration**: store processed data in the weather_data database for further utilization and reporting.

## Queries

### PREDICTION
Hourly forecast for the municipality passed as a parameter: **municipio**. Provides hourly information up to 48 hours.

Queries are performed starting from 20:10 to obtain values for the next 24 hours of the following day.

```sh
/api/prediccion/especifica/municipio/horaria/{municipio}
```

There's another similar query:
```sh
/api/prediccion/especifica/municipio/diaria/{municipio}
```
But it provides the same response data.


### METEO
Returns a summary of weather values taken at a certain weather station on the specified date. In this case we have to put limits in the date so that they are 24 hours and coincide with the data of the prediction query.

```sh
api/valores/climatologicos/diarios/datos/fechaini/2024-06-03T00%3A00%3A00UTC/fechafin/2024-06-03T23%3A59%3A59UTC/estacion/{INDICATIVO}
```

It' i's necessary to ask for data from 4-5 days before, since some stations may not have them ready until then.

## Selected cities and towns
Municipalities with nearby meteorological stations and low accumulated precipitation percentages in recent years.

Areas with the highest risk of desertification and/or drought:

Accumulated Precipitation in the Hydrological Year (2024)
![Accumulated Precipitation in the Hydrological Year (2024)](images/accumulated-precipitation-2024.png)

Spain-Aridity Index
![Spain-Aridity Index](images/spain-aridity-index.png)

Cold Drop
![Spain-Aridity Index](images/cold-drop.png)

## Running the Project

### 1. Setup Database 

Create the `weather_station` database:

```sh
psql
```

```sql
CREATE DATABASE weather_station;
```
```sh
\c weather_station;
```
Copy and run the content of `weather_spain_db.sql` in the terminal.


### 2. Config database file

Configure your database connection details in `config.ini`:
```conf
[database]
dbname = weather_station
user = jfgs
password = ******
host = localhost
port = 5432
```

### 3. Configuring and Running the Airflow DAG

The two scripts to be automated to run daily are:

- `weather_data.py`: script to query the AEMET API, to retrieve the data, to check for errors when fetching from any of the stations, and to save the data in .json files in the /data folder. The JSON files are stored in two subfolders depending on whether it's from a **meteo** (5 days before) or **prediction** (next day) query.

- `weather_etl_job.py`: Script to read the JSON files from the current date, perform calculations, and save them in the `weather_data` database.

Copy the file `weather_data_pipeline_dag.py` in folder `/airflow/dags`.

In two different terminals:
```sh
airflow webserver --port 8080
```
```sh
airflow scheduler
```

Go to http://localhost:8080/ .


## API AEMET
Information about the API:
https://opendata.aemet.es/dist/index.html