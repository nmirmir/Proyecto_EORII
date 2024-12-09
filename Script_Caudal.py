import time as tm
from asyncua.sync import Server
import json

def data_collection(data):
    Caudal = {
        "caudal_m3_s_5": [],
        "caudal_m3_s_60": [],
        "Estado": []
    }
    contador = 0
    acum_caudal = 0
    
    for i in data:
        contador += 1
        # Handle both string and float inputs
        caudal = i["caudal_m3_s"]
        if isinstance(caudal, str):
            caudal = float(caudal.replace(",", "."))
        else:
            caudal = float(caudal)
            
        acum_caudal += caudal
        
        if contador == 12:  # 12 readings of 5 minutes = 1 hour
            Caudal["caudal_m3_s_60"].append(acum_caudal / 12)  # Average for the hour
            contador = 0
            acum_caudal = 0
            
        Caudal["caudal_m3_s_5"].append(caudal)
        Caudal["Estado"].append(i["estado"])
    
    return Caudal

def init_server():
    servidor = Server()
    
    from asyncua import ua
    servidor.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    servidor.set_server_name("OPC UA Caudal Server")
    
    servidor.set_endpoint("opc.tcp://LAPTOP-QJA0AD04:53530/OPCUA/SimulationServer")
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

def data_sending(idx, servidor, Caudal):
    mi_obj = servidor.nodes.objects.add_object(idx, "Objeto_Caudal")
    print(f"NodeId del objeto creado: {mi_obj.nodeid}")
    tm.sleep(1)
    
    from asyncua import ua
    
    # Create variables with proper data types using variant type
    # Caudal_5 as Float
    Caudal_5 = mi_obj.add_variable(idx, "Caudal_m3_s_5", 
                                  ua.Variant(float(Caudal["caudal_m3_s_5"][0]), 
                                           ua.VariantType.Float))
    
    # Caudal_60 as Float
    Caudal_60 = mi_obj.add_variable(idx, "Caudal_m3_s_60", 
                                   ua.Variant(float(Caudal["caudal_m3_s_60"][0]), 
                                            ua.VariantType.Float))
    
    # Estado as String
    Estado = mi_obj.add_variable(idx, "Estado", 
                                ua.Variant(str(Caudal["Estado"][0]), 
                                         ua.VariantType.String))
    
    Caudal_5.set_writable()
    Caudal_60.set_writable()
    Estado.set_writable()
    
    return Caudal_60, Caudal_5, Estado

def iterative_data(Caudal, Caudal_5, Caudal_60, Estado):
    from asyncua import ua
    
    for index in range(len(Caudal["caudal_m3_s_5"])):
        tm.sleep(1)
        # Create variants with proper types for each value
        C5 = ua.Variant(float(Caudal["caudal_m3_s_5"][index]), ua.VariantType.Float)
        
        if index % 12 == 0 and index // 12 < len(Caudal["caudal_m3_s_60"]):
            C60 = ua.Variant(float(Caudal["caudal_m3_s_60"][index // 12]), ua.VariantType.Float)
        else:
            C60 = None
        
        estado = ua.Variant(str(Caudal["Estado"][index]), ua.VariantType.String)
        
        print(f"Caudal 5min: {C5.Value}, Caudal 60min: {C60.Value if C60 is not None else None}, Estado: {estado.Value}")
        Caudal_5.write_value(C5)
        if C60 is not None:
            Caudal_60.write_value(C60)
        Estado.write_value(estado)

if __name__ == "__main__":
    servidor = None
    try:
        print("Starting OPC UA Server...")
        
        # Load JSON data
        with open('poyo.json', 'r') as file:
            data = json.load(file)

        # Initialize and start server
        idx, servidor = init_server()
        servidor.start()
        print("Server started successfully!")

        # Process and send data
        print("Processing caudal data...")
        Caudal = data_collection(data)
        
        print("Setting up OPC UA variables...")
        Caudal_60, Caudal_5, Estado = data_sending(idx, servidor, Caudal)
        
        print("Starting data iteration...")
        iterative_data(Caudal, Caudal_5, Caudal_60, Estado)

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        if servidor:
            print("Stopping server...")
            servidor.stop()
            print("Server stopped.")