import time as tm
from asyncua.sync import Server
import json



def data_collection(data): #funcion para sacar el dato de hora del documento en json
    Horario = {"Fechas":[],"Horas":[]}
    for i in data:
        Horario["Fechas"].append(i["fecha"])
        Horario["Horas"].append(i["hora"])
        tm.sleep(1)
        print(Horario["Fechas"].append(i["fecha"]),
        Horario["Horas"].append(i["hora"]))
    return Horario

def init_server():
    # Create and configure the server
    servidor = Server()
    servidor.set_endpoint("opc.tcp://DESKTOP-M1F986I:53530/OPCUA/SimulationServer")
    uri = "http://www.epsa.upv.es/entornos/NJFJ"
    idx = servidor.register_namespace(uri)
    return idx,servidor

def data_sending(idx,servidor,Horario):
    # Add an object and a writable variable
    mi_obj = servidor.nodes.objects.add_object(idx, "Objeto_Horario")
    fecha = mi_obj.add_variable(idx, "Fecha", Horario["Fechas"][0])
    hora = mi_obj.add_variable(idx, "Hora", Horario["Horas"][1])
    hora.set_writable()
    fecha.set_writable()
    return fecha,hora

def iterative_data(Horario,hora,fecha):
    for index,f in enumerate(Horario["Fechas"]):
        tm.sleep(1)
        f = Horario["Fechas"][index]
        h = Horario["Horas"][index]
        print(f,h)
        fecha.write_value(f)
        hora.write_value(h)



if __name__ == "__main__":
    servidor = None  # Initialize servidor to None
    try:
        # Cargamos el JSON
        with open('data.json', 'r') as file:
            data = json.load(file)

        # inciamos el server
        idx, servidor = init_server()

        # Metemos los datos en un diccionario que almacena listas
        Horario = data_collection(data)
        
        # Creamos objeto y las variables de fech y hora, les damos un valor inicial y hacemos que se puedan modificar
        fecha, hora = data_sending(idx, servidor, Horario)

        # Modificamos los valore de las variables
        iterative_data(Horario, hora, fecha)

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        if servidor:  # Check if servidor is not None before stopping
            servidor.stop()  # Ensure the server stops when exiting
