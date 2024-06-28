# weather-spain-pipeline

## Base de datos

CREATE DATABASE weather_station;

\c weather_station;

CREATE TABLE CITY_STATION (
    id SERIAL PRIMARY KEY,
    city_code VARCHAR(100) NOT NULL,
    station_code VARCHAR(100) NOT NULL,
    city_name VARCHAR(100) NOT NULL,
    station_name VARCHAR(100) NOT NULL
);


## config.ini
```conf
[database]
dbname = weather_station
user = jfgs
password = ####
host = localhost
port = 5432
```

## Crontab
```sh
crontab -e
```

```
0 20 * * * cd /home/jfgs/Projects/weather-spain && /usr/bin/python3 app.py
```

```sh
crontab -l
```

## License
```
This README provides a comprehensive overview of the project, the technologies used, the project structure, setup instructions, steps to run the ETL workflow, and how to contribute to the project.
```

## AEMET
Citar a AEMET como fuente de la información

# Consultas:

## PREDICTION
Predicción horaria para el municipio que se pasa como parámetro (municipio). Presenta la información de hora en hora hasta 48 horas.
/api/prediccion/especifica/municipio/horaria/{municipio}


## METEO
api/valores/climatologicos/diarios/datos/fechaini/2024-06-03T00%3A00%3A00UTC/fechafin/2024-06-03T23%3A59%3A59UTC/estacion/{INDICATIVO}

modificar fechas, los datos son de tres días antes.

## Cities
Ciudades estudio:
- municipios que son capital de provincia o con una población superior a 60.000 habitantes en la España peninsular con un porcentaje de precipitación acumulada baja, una estación meteorológica cerca y con bajas precicipitaciones en los últimos años.


