import time as tm
from asyncua.sync import Server
import json



def data_collection(data): #funcion para sacar el dato de hora del documento en json
    Horario = {"Fechas":[],"Horas":[]}
    for i in data:
        Horario["Fechas"].append(i["fecha"])
        Horario["Horas"].append(i["hora"])
    return Horario

def init_server():
    # Create and configure the server
    servidor = Server()
    
    # Configure security settings
    from asyncua import ua
    servidor.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    servidor.set_server_name("OPC UA Simulation Server")
    
    # Configure endpoint
    servidor.set_endpoint("opc.tcp://LAPTOP-PIE5PVF8:53530/OPCUA/SimulationServer") #IP Jaime
    #servidor.set_endpoint("opc.tcp://DESKTOP-M1F986I:53530/OPCUA/SimulationServer ") #IP Nicolas

    
    # Configure authentication
    servidor.set_security_IDs(["Anonymous"])
    
    uri = "http://www.epsa.upv.es/entornos/NJFJ"
    idx = servidor.register_namespace(uri)
    print(f'nuestro idx: {idx}')
    tm.sleep(1)
    return idx, servidor

def data_sending(idx,servidor,Horario):
    # Add an object and a writable variable
    mi_obj = servidor.nodes.objects.add_object(idx, "Objeto_Horario")
    print(f"NodeId del objeto creado: {mi_obj.nodeid}")
    tm.sleep(1)
    fecha = mi_obj.add_variable(idx, "Fecha", Horario["Fechas"][0])
    hora = mi_obj.add_variable(idx, "Hora", Horario["Horas"][0])
    hora.set_writable()
    fecha.set_writable()
    hora.set_writable()
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
    servidor = None
    try:
        print("Starting OPC UA Server...")
        
        # Load JSON data
        with open('chiva.json', 'r') as file:
            data = json.load(file)

        # Initialize and start server
        idx, servidor = init_server()
        servidor.start()
        print("Server started successfully!")
        tm.sleep(0.1)

        # Process and send data
        print("Processing data...")
        Horario = data_collection(data)
        
        print("Setting up OPC UA variables...")
        fecha, hora = data_sending(idx, servidor, Horario)
        print("Starting data iteration...")
        iterative_data(Horario, hora, fecha)

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        if servidor:
            print("Stopping server...")
            servidor.stop()
            print("Server stopped.")
