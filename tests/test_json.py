import json

# Cargar los datos JSON desde el archivo
file_path = 'data/28-06-2024/29001-prediction-28-06-2024.json'

with open(file_path, 'r') as file:
    data = json.load(file)

    temperatures = data[0]['prediccion']['dia'][1]['temperatura']

    values = []

    for temp in temperatures:
        values.append(float(temp['value']))  

    print(values)

    