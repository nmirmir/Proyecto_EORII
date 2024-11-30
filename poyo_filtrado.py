import pandas as pd
import json

# Leer el archivo CSV
df = pd.read_csv('data.csv')

# Crear una lista para almacenar los datos transformados
data_list = []

# Iterar sobre cada fila del DataFrame
for index, row in df.iterrows():
    # Leer los datos de la primera, segunda y tercera columna
    fecha_hora = row.iloc[0]  # Primera columna
    caudal_m3_s = row.iloc[1]  # Segunda columna
    estado_sensor = row.iloc[2]  # Tercera columna
    
    # Verificar si la fecha y hora es algo?¿
    if pd.isna(fecha_hora):
        fecha = None
        hora = None
    else:
        # Separar la fecha - la hora
        fecha = pd.to_datetime(fecha_hora).date().isoformat()
        hora = pd.to_datetime(fecha_hora).time().isoformat()
    
    # Verificar si hay un fallo en el sensor -si es 'FALLO'
    if estado_sensor == 'FALLO':
        estado = 'FALLO'
        caudal_m3_s = 0  
        # Cambiar el caudal a 0 en lugar de FALLO , esto lo hago mas que otra cosa para hacer como si el sensor 'muere' o falla
    else:
        estado = True
    
    # Crear un diccionario con los datos transformados
    data_dict = {
        'fecha': fecha,
        'hora': hora,
        'caudal_m3_s': caudal_m3_s,
        'estado': estado
    }
    
    # Agregar el diccionario a la lista
    data_list.append(data_dict)

# Guardar los datos en un archivo JSON
with open('poyo.json', 'w') as json_file:
    json.dump(data_list, json_file, indent=4)

print("Los datos han sido transformados y guardados en poyo.json.")