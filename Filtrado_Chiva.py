# -*- coding: utf-8 -*-

import pandas as pd
import json
from datetime import datetime

# Leer el archivo Excel
df = pd.read_excel('data.xlsx')

# Crear una lista para almacenar los datos transformados
data_list = []

# Iterar sobre cada fila del excel
for index, row in df.iterrows():
    # Leer los datos de la primera y segunda columna
    fecha_hora = row.iloc[0]  
    pluvimetro_mm = row.iloc[1]  
    
    # Verificar si la fecha y hora están presentes
    if pd.isna(fecha_hora):
        fecha_hora_iso = None
    else:
        # Combinar la fecha y la hora en un solo campo en formato ISO 8601
        fecha_hora_iso = fecha_hora.isoformat()
    
    # Verificar si hay un fallo en el sensor
    if pluvimetro_mm == 'FALLO':
        estado = False
        pluvimetro_mm = None
    else:
        estado = True
        pluvimetro_mm = float(pluvimetro_mm)
    
    # Crear un diccionario con los datos
    data_dict = {
        'FechaHora': fecha_hora_iso,
        'Pluvimetro (mm)': pluvimetro_mm,
        'Estado': estado
    }
    
    # Agregar el diccionario a la lista
    data_list.append(data_dict)

# Guardar los datos en un archivo JSON
with open('chiva.json', 'w') as json_file:
    json.dump(data_list, json_file, indent=4)

print("Los datos han sido transformados y guardados en chiva.json. :D")