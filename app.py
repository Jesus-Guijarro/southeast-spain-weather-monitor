import requests
import configparser
import psycopg2
from datetime import datetime, timedelta
import os

# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Get the configuration values
db_config = config['database']
DB_NAME = db_config['dbname']
DB_USER = db_config['user']
DB_PASSWORD = db_config['password']
DB_HOST = db_config['host']
DB_PORT = db_config['port']

# Conexión a la base de datos
conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
cursor = conn.cursor()

query = "SELECT city_code, station_code FROM CITY_STATION;"

# Ejecutar la consulta
cursor.execute(query)

# Obtener todos los resultados
rows = cursor.fetchall()

# Cerrar cursor y conexión
cursor.close()
conn.close()

url_prediccion = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{city_code}"
url_climatologicos = "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{fecha_inicio}/fechafin/{fecha_fin}/estacion/{station_code}"

with open('api.txt', 'r') as file:
    api_key = file.read()

querystring = {"api_key":api_key}

headers = {
    'cache-control': "no-cache",
    'accept': "application/json"
    }

def create_folder():
    current_date = datetime.now()
    date_folder_name = current_date.strftime("%d-%m-%Y")
    folder_path = f"./{date_folder_name}"
    os.makedirs(folder_path, exist_ok=True)

    return folder_path

# Función para hacer la solicitud y obtener la URL de los datos reales
def get_data_url(url):
    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        data = response.json()
        if 'datos' in data:
            return data['datos']
        else:
            print(f"Error: 'datos' no encontrado en la respuesta")
            return None
    else:
        print(f"Error {response.status_code}")
        return None
    
def fetch_and_save(url, filename):
    data_url = get_data_url(url)
    if data_url:
        response = requests.get(data_url)
        if response.status_code == 200:
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(response.text)
        else:
            print(f"Error {response.status_code} al solicitar {data_url}")

folder_path = create_folder()
for row in rows:
    city_code, station_code = row

    print(city_code, station_code)
    
    # Construir URL para la API de predicción horaria
    api_url_prediccion = url_prediccion.format(city_code=city_code)

    prediccion_file_name = f"{folder_path}/predic-{datetime.now().strftime('%d-%m-%Y')}-{city_code}.txt"

    fetch_and_save(api_url_prediccion, prediccion_file_name)



    # Generar la fecha 4 días antes del día actual en el formato requerido
    fecha_actual = datetime.now()
    fecha = fecha_actual - timedelta(days=4)
    
    # Formatear las fechas en el formato necesario por la API de climatológicos
    fecha_inicio_str = fecha.strftime("%Y-%m-%dT00:00:00UTC")
    fecha_fin_str = fecha.strftime("%Y-%m-%dT23:59:59UTC")
    
    # Construir URL para la API de valores climatológicos diarios
    api_url_climatologicos = url_climatologicos.format(fecha_inicio=fecha_inicio_str, fecha_fin=fecha_fin_str, station_code=station_code)
    
    # Realizar la solicitud GET a la API de valores climatológicos diarios

    meteo_file_name = f"{folder_path}/meteo-{datetime.now().strftime('%d-%m-%Y')}-{station_code}.txt"

    fetch_and_save(api_url_climatologicos, meteo_file_name)

