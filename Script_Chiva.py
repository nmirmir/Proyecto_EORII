import time as tm
from asyncua.sync import Server
import json



def data_collection(data): #funcion para sacar el dato de hora del documento en json
    Aforo = {"aforo_mm_5":[],"aforo_mm_60":[],"Estado":[]}
    contador = 0
    acum_aforo_mm = 0
    for i in data:
        contador += 1
        acum_aforo_mm += i["aforo_mm"]
        if contador == 12:
            Aforo["aforo_mm_60"].append(i["aforo_mm"]) = acum_aforo_mm
            contador = 0
            acum_aforo_mm = 0
        Aforo["aforo_mm_5"].append(i["aforo_mm"])
        Aforo["Estado"].append(i["estado"])
    return Aforo

def init_server():
    # Create and configure the server
    servidor = Server()
    
    # Configure security settings
    from asyncua import ua
    servidor.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    servidor.set_server_name("OPC UA Simulation Server")
    
    # Configure endpoint
    #servidor.set_endpoint("opc.tcp://LAPTOP-PIE5PVF8:53530/OPCUA/SimulationServer") #IP Jaime
    servidor.set_endpoint("opc.tcp://DESKTOP-M1F986I:53530/OPCUA/SimulationServer ") #IP Nicolas
    # Configure authentication
    servidor.set_security_IDs(["Anonymous"])
    
    uri = "http://www.epsa.upv.es/entornos/NJFJ"
    idx = servidor.register_namespace(uri)
    print(f'nuestro idx: {idx}')
    tm.sleep(1)
    return idx, servidor

def data_sending(idx,servidor,Horario):
    # Add an object and a writable variable
    mi_obj = servidor.nodes.objects.add_object(idx, "Objeto_Aforo")
    print(f"NodeId del objeto creado: {mi_obj.nodeid}")
    tm.sleep(1)
    Aforo_mm_5 = mi_obj.add_variable(idx, "Aforo_mm_5", Horario["aforo_mm_5"][0])
    Aforo_mm_60 = mi_obj.add_variable(idx, "Aforo_mm_60", Horario["aforo_mm_60"][0])
    Estado = mi_obj.add_variable(idx, "Estado", Horario["Estado"][0])
    Aforo_mm_5.set_writable()
    Aforo_mm_60.set_writable()
    Estado.set_writable()
    return Aforo_mm_60,Aforo_mm_5,Estado

def iterative_data(Aforo,Aforo_mm_5,Aforo_mm_60,Estado):
    for index in range(len(Aforo["aforo_mm_5"])):
        tm.sleep(1)
        A5 = Aforo["aforo_mm_5"][index]
        A60 = Aforo["aforo_mm_60"][index]
        Estado = Aforo["Estado"][index]
        print(A5,A60,Estado)
        Aforo_mm_5.write_value(A5)
        Aforo_mm_60.write_value(A60)
        Estado.write_value(Estado)


if __name__ == "__main__":
    servidor = None
    try:
        print("Starting OPC UA Server...")
        
        # Load JSON data
        with open('data.json', 'r') as file:
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
