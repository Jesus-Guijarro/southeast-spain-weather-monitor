import json
from datetime import datetime, timedelta

import psycopg2

# Función para leer el archivo JSON
def read_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

# Función para procesar los datos meteorológicos
def process_meteorological_data(data):
    # Extraer la fecha de "elaborado"
    elaborado_date_str = data['elaborado']
    elaborado_date = datetime.strptime(elaborado_date_str, "%Y-%m-%dT%H:%M:%SZ")

    # Procesar las secciones de "temperatura"
    temperaturas = data.get('temperatura', [])
    
    # Crear una lista con las temperaturas y sus horas correspondientes
    temperatura_data = []
    for temp in temperaturas:
        periodo = int(temp['periodo'])
        value = temp['value']
        
        # Calcular la fecha y hora correspondientes a este periodo
        temp_date_time = elaborado_date + timedelta(hours=periodo)
        
        # Añadir a la lista
        temperatura_data.append({
            'datetime': temp_date_time,
            'value': value
        })

    # Ordenar la lista por fecha y hora (aunque debería estar ya ordenada)
    temperatura_data = sorted(temperatura_data, key=lambda x: x['datetime'])
    
    return temperatura_data

# Función para mostrar los datos procesados
def print_temperatura_data(temperatura_data):
    for temp in temperatura_data:
        print(f"Fecha y hora: {temp['datetime']} - Temperatura: {temp['value']}")

# Ruta del archivo JSON
json_file_path = 'path/to/your/meteorological_data.json'

# Leer y procesar los datos
data = read_json_file(json_file_path)
temperatura_data = process_meteorological_data(data)

# Mostrar los datos procesados
print_temperatura_data(temperatura_data)
