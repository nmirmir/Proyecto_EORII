import csv
import json
from datetime import datetime

# Función para convertir los datos a JSON
def convert_data_to_json(input_file, output_file):
    data = []
    
    with open(input_file, 'r') as infile:
        reader = csv.reader(infile)
        
        # Saltar el encabezado en el archivo de entrada
        next(reader)
        
        for row in reader:
            # Combinar la fecha y la hora en un solo campo en formato ISO 8601
            fecha_hora = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S').isoformat()
            
            # Convertir la segunda columna a float (considerando la coma como separador decimal y manejando valores vacíos)
            caudal = float(row[1].replace(',', '.')) if row[1] else None
            
            # Convertir la tercera columna a booleano
            estado = False if row[2].strip().upper() == 'FALLO' else True
            
            # Añadir los datos convertidos a la lista
            data.append({
                'FechaHora': fecha_hora,
                'Caudal (m3/s)': caudal,
                'Estado': estado
            })
    
    # Escribir los datos en un archivo JSON
    with open(output_file, 'w') as outfile:
        json.dump(data, outfile, indent=4)

# Convertir los datos de 'data.csv' a 'poyo.json'
convert_data_to_json('data.csv', 'poyo.json')

print("La conversión se ha completado con éxito y se ha guardado en 'poyo.json'.")