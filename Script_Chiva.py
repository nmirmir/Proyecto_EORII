import time as tm
from asyncua.sync import Server, Client
import json



def data_collection(data): #funcion para sacar el dato de hora del documento en json
    Pluviometro = {"pluviometro_mm_5":[[],[],[]],"pluviometro_mm_60":[],"Estado":[[],[],[]]}
    contador = 0
    acum_pluviometro_mm = 0
    for i in data:
        contador += 1
        acum_pluviometro_mm += i["pluviometro_mm"]
        if contador == 12:
            Pluviometro["pluviometro_mm_60"].append(acum_pluviometro_mm)
            contador = 0
            acum_pluviometro_mm = 0
        Pluviometro["pluviometro_mm_5"][0].append(i["fecha"])
        Pluviometro["pluviometro_mm_5"][1].append(i["hora"])
        Pluviometro["pluviometro_mm_5"][2].append(i["pluviometro_mm"])
        Pluviometro["Estado"][0].append(i["fecha"])
        Pluviometro["Estado"][1].append(i["hora"])
        Pluviometro["Estado"][2].append(i["estado"])
    return Pluviometro

def init_server():
    # Create and configure the server
    servidor = Server()
    
    # Configure security settings
    from asyncua import ua
    servidor.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    servidor.set_server_name("OPC UA Simulation Server Pluviometro")
    
    # Configure endpoint
    servidor.set_endpoint("opc.tcp://DESKTOP-M1F986I:53540/OPCUA/PluviometroServer")
    # Configure authentication
    servidor.set_security_IDs(["Anonymous"])
    
    uri = "http://www.epsa.upv.es/entornos/NJFJ"
    idx = servidor.register_namespace(uri)
    print(f'nuestro idx: {idx}')
    tm.sleep(1)
    return idx, servidor

def hour_server():
    hour_server = "opc.tcp://LAPTOP-PIE5PVF8:53530/OPCUA/SimulationServer"
    node_id_fecha = "ns=2;i=2"
    node_id_hora = "ns=2;i=3"
    return hour_server, node_id_fecha, node_id_hora

def data_sending(idx,servidor,Pluviometro):
    # Add an object and a writable variable
    mi_obj = servidor.nodes.objects.add_object(idx, "Objeto_Pluviometro")
    print(f"NodeId del objeto creado: {mi_obj.nodeid}")
    tm.sleep(1)
    Pluviometro_mm_5 = mi_obj.add_variable(idx, "Pluviometro_mm_5", Pluviometro["pluviometro_mm_5"][2][0])
    Pluviometro_mm_60 = mi_obj.add_variable(idx, "Pluviometro_mm_60", Pluviometro["pluviometro_mm_60"][0])
    Estado = mi_obj.add_variable(idx, "Estado", Pluviometro["Estado"][2][0])
    Pluviometro_mm_5.set_writable()
    Pluviometro_mm_60.set_writable()
    Estado.set_writable()
    return Pluviometro_mm_60,Pluviometro_mm_5,Estado

def actual_hour_data(hour_server_url, node_id_fecha, node_id_hora):
    with Client("opc.tcp://LAPTOP-PIE5PVF8:53530/OPCUA/SimulationServer") as client:
        try: 
            client.connect()
            print(f"Conectado al servidor HORA - OPC UA en: {hour_server_url}")
            fecha_node = client.get_node(node_id_fecha)
            hora_node = client.get_node(node_id_hora)
            print(f"Hora NodeId válido: {hora_node.nodeid}")
            print(f"Fecha NodeId válido: {fecha_node.nodeid}")
            fecha_actual = fecha_node.read_value()
            #fecha_actual = fecha_node.get_value()
            #fecha_actual = client.get_values(fecha_node)
            #fecha_actual = client.get_node(node_id_fecha).read_value()
            print(f"Valor actual del nodo Fecha: {fecha_actual}")
            hora_actual = hora_node.read_value()
            #hora_actual = hora_node.get_value()
            #hora_actual = client.get_values(hora_node)
            #hora_actual = client.get_node(node_id_hora).read_value()
            print(f"Valor actual del nodo Hora: {hora_actual}")
            print(f"Datos obtenidos del primer servidor - Fecha: {fecha_actual}, Hora: {hora_actual}")
            return fecha_actual, hora_actual
        except Exception as e:
            print(f"Error al obtener los nodos: {e}")
            return None, None

def iterative_data(Pluviometro, Pluviometro_mm_5, Pluviometro_mm_60, Estado, hour_server_url, node_id_fecha, node_id_hora):
    #for index in range(len(Pluviometro["pluviometro_mm_5"][0])):
    while True:
        tm.sleep(1)
        fecha_actual, hora_actual = actual_hour_data(hour_server_url, node_id_fecha, node_id_hora)
        if fecha_actual == None:
            break
        i = 0
        try:
            while fecha_actual != Pluviometro["pluviometro_mm_5"][0][i] and hora_actual != Pluviometro["pluviometro_mm_5"][1][i]:
                i += 1
                if i > len(Pluviometro["pluviometro_mm_5"][0]):
                    print("Hora no encontrada")
                    raise Exception("Hora / Fecha no encontradas")
            P5 = Pluviometro["pluviometro_mm_5"][2][i]
            #A60 = Pluviometro["pluviometro_mm_60"][2][i]
            Estado = Pluviometro["Estado"][2][i]
            print(P5,Estado)
            Pluviometro_mm_5.write_value(P5)
            #Pluviometro_mm_60.write_value(A60)
            if Estado == True:
                Estado.write_value(1)
            else:
                Estado.write_value(1)
        except Exception as e:
            print(f"Excpecion interceptada:{e}")

if __name__ == "__main__":
    servidor = None
    try:
        print("Starting OPC UA Servers...")
        ip_server_hora = "opc.tcp://DESKTOP-M1F986I:53530/OPCUA/SimulationServer"
        ip_server_pluviometro = "opc.tcp://DESKTOP-M1F986I:53540/OPCUA/PluviometroServer"
        # Load JSON data
        with open('chiva.json', 'r') as file:
            data = json.load(file)

        # Initialize and start server
        idx, servidor_pluviometro = init_server()
        hour_server_url, node_id_fecha, node_id_hora = hour_server()
        servidor_pluviometro.start()
        print("Server started successfully!")

        # Process and send data
        print("Processing data (pluviometro)...")
        Pluviometro = data_collection(data)
        
        print("Setting up OPC UA variables...")
        Pluviometro_mm_60, Pluviometro_mm_5, Estado = data_sending(idx, servidor_pluviometro, Pluviometro)
        print("Starting data iteration...")

        iterative_data(Pluviometro, Pluviometro_mm_5, Pluviometro_mm_60, Estado,hour_server_url, node_id_fecha, node_id_hora)

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        if servidor:
            print("Stopping server...")
            servidor.stop()
            print("Server stopped.")
