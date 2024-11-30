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
    servidor.set_endpoint("opc.tcp://LAPTOP-PIE5PVF8:53530/OPCUA/CaudalServer") #IP Jaime
    #servidor.set_endpoint("opc.tcp://DESKTOP-M1F986I:53530/OPCUA/SimulationServer ") #IP Nicolas
    # Configure authentication
    servidor.set_security_IDs(["Anonymous"])
    
    uri = "http://www.epsa.upv.es/entornos/NJFJ/Caudal"
    idx = servidor.register_namespace(uri)
    print(f'nuestro idx: {idx}')
    tm.sleep(1)
    return idx, servidor

def hour_server():
    hour_server = "opc.tcp://DESKTOP-M1F986I:53530/OPCUA/SimulationServer"
    node_id_fecha = "ns=2;s=Objeto_Horario.Fecha"
    node_id_hora = "ns=2;s=Objeto_Horario.Hora"
    return hour_server, node_id_fecha, node_id_hora

def data_sending(idx,servidor,Aforo):
    # Add an object and a writable variable
    mi_obj = servidor.nodes.objects.add_object(idx, "Objeto_Aforo")
    print(f"NodeId del objeto creado: {mi_obj.nodeid}")
    tm.sleep(1)
    Aforo_mm_5 = mi_obj.add_variable(idx, "Aforo_mm_5", Aforo["aforo_mm_5"][0])
    Aforo_mm_60 = mi_obj.add_variable(idx, "Aforo_mm_60", Aforo["aforo_mm_60"][0])
    Estado = mi_obj.add_variable(idx, "Estado", Aforo["Estado"][0])
    Aforo_mm_5.set_writable()
    Aforo_mm_60.set_writable()
    Estado.set_writable()
    return Aforo_mm_60,Aforo_mm_5,Estado

def actual_data(hour_server_url, node_id_fecha, node_id_hora):
    with Client(hour_server_url) as client:
        print(f"Conectado al servidor HORA - OPC UA en: {client_url}")
        fecha_node = client.get_node(node_id_fecha)
        hora_node = client.get_node(node_id_hora)
        fecha_actual = fecha_node.read_value()
        hora_actual = hora_node.read_value()
        print(f"Datos obtenidos del primer servidor - Fecha: {fecha_actual}, Hora: {hora_actual}")
        return fecha_actual, hora_actual

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
        print("Starting OPC UA Servers...")
        
        # Load JSON data
        with open('data.json', 'r') as file:
            data = json.load(file)

        # Initialize and start server
        idx, servidor_aforo = init_server()
        hour_server_url, node_id_fecha, node_id_hora = hour_server()
        servidor_aforo.start()
        print("Server started successfully!")

        # Process and send data
        print("Processing data (aforo)...")
        Aforo = data_collection(data)
        
        print("Setting up OPC UA variables...")
        Aforo_mm_60, Aforo_mm_5, Estado = data_sending(idx, servidor, Aforo)
        print("Starting data iteration...")

        iterative_data(Aforo, Aforo_mm_5, Aforo_mm_60,Estado)

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        if servidor:
            print("Stopping server...")
            servidor.stop()
            print("Server stopped.")
