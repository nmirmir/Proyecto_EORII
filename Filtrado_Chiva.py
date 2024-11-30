import pandas as pd
import json

# Leer el archivo Excel
df = pd.read_excel('data.xlsx')

# Crear una lista para almacenar los datos transformados
data_list = []

# Iterar sobre cada fila del excel
for index, row in df.iterrows():
    # Leer los datos de la primera y segunda columna
    fecha_hora = row.iloc[0]  
    # Primera columna
    aforo_mm = row.iloc[1]  
    # Segunda columna
    

    # Verificar si la fecha y (hora estan?¿)
    if pd.isna(fecha_hora):
        fecha = None
        hora = None
    else:
        # Separar la fecha - la hora
        fecha = fecha_hora.date().isoformat()
        hora = fecha_hora.time().isoformat()
    
    # Verificar si hay un fallo en el sensor
    if aforo_mm == 'FALLO':
        estado = 'FALLO'
        aforo_mm = None
    else:
        estado = True
    
    # Crear un diccionario con los datos
    data_dict = {
        'fecha': fecha,
        'hora': hora,
        'aforo_mm': aforo_mm,
        'estado': estado
    }
    
    # Agregar el diccionario a la lista
    data_list.append(data_dict)

# Guardar los datos en un archivo JSON
with open('chiva.json', 'w') as json_file:
    json.dump(data_list, json_file, indent=4)



print("Los datos han sido transformados y guardados en chiva.json. :D ")
