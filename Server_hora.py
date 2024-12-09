import time
from asyncua.sync import Server
from asyncua import ua
import json
import datetime
import xml.etree.ElementTree as ET
multiplicador = 20

class Server_hora:
    def __init__(self):
        self.server = Server()
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        self.server.set_server_name("OPC UA Simulation Server")
        self.server.set_endpoint("opc.tcp://LAPTOP-QJA0AD04:53540/OPCUA/ServerHora")
        self.server.set_security_IDs(["Anonymous"])
        self.uri = "http://www.epsa.upv.es/entornos/NJFJ"
        self.idx = self.server.register_namespace(self.uri)
        print(f'nuestro idx: {self.idx}')
        time.sleep(1)
        self.timestamps = self.process_json_timestamps("poyo.json")
        # Add objects folder
        self.objects = self.server.nodes.objects
        # Create device folder
        self.devices_folder = self.objects.add_folder(self.idx, "Devices")

    def run(self):
        self.server.start()
        print("Server started successfully!")
        self.fechaHora,self.multiplicador = self.setupVariables()
        self.server_running = True

        for timestamp in self.timestamps:
            if self.server_running:
                self.send_data(timestamp,self.multiplicador)
                time.sleep(1.0 / self.multiplicador.get_value())
            else:
                break   

    def process_json_timestamps(self,json_file_path): ####### Funciona
        # Read JSON file
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        
        # Convert each date+time combination to datetime objects
        timestamps = []
        for reading in data:
            date_str = reading['FechaHora']
            # Combine date and time strings and convert to datetime
            timestamp = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
            timestamps.append(timestamp)        
        return timestamps
    
    def setupVariables(self):
        # Load XML model
        root = self.load_model()
        ns = {'ns': 'http://opcfoundation.org/UA/2011/03/UANodeSet.xsd'}
        
        # Get S_Hora device from XML
        equipment = root.find('.//ns:Equipment', ns)
        hora_device = equipment.find(".//ns:Device[@id='S_Hora']", ns)
        
        # Create root folder using device name from XML
        device_name = hora_device.find('ns:Name', ns).text
        folder_horario = self.devices_folder.add_folder(self.idx, device_name)
        
        # Create the object using device description
        device_description = hora_device.find('ns:Description', ns).text
        objHorario = folder_horario.add_object(self.idx, device_description)
        
        # Add variables based on XML DataPoints
        datapoints = hora_device.find('ns:DataPoints', ns)
        
        # Create Hora (DateTime) variable
        hora_dp = datapoints.find(".//ns:DataPoint[@id='Hora']", ns)
        fechaHora = objHorario.add_variable(
            self.idx,
            hora_dp.find('ns:Name', ns).text,
            self.timestamps[0],
            ua.VariantType.DateTime
        )
        
        # Create Multiplicador variable
        multiplicador_dp = datapoints.find(".//ns:DataPoint[@id='Multiplicador']", ns)
        multiplicador = objHorario.add_variable(
            self.idx,
            multiplicador_dp.find('ns:Name', ns).text,
            float(multiplicador_dp.find('ns:Value', ns).text),
            ua.VariantType.Float
        )
        
        fechaHora.set_writable()
        multiplicador.set_writable()
        return fechaHora, multiplicador

    def send_data(self,timestamp,multiplicador):
        v_m = multiplicador.get_value()
        float_v_m = ua.Float(v_m)
        print(multiplicador.get_value())
        print(timestamp)
        self.fechaHora.write_value(timestamp)
        self.multiplicador.write_value(float_v_m)
        
    def load_model(self): ####### Funciona
        tree = ET.parse('Modelo_datos.xml')
        return tree.getroot()

if __name__ == "__main__":
    print("-------------------")
    server = Server_hora()
    root = server.load_model()
    print(root)
    root = server.load_model()
    
    # Define the namespace
    ns = {'ns': 'http://opcfoundation.org/UA/2011/03/UANodeSet.xsd'}
    
    # Get Equipment node with namespace
    equipment = root.find('.//ns:Equipment', ns)
    
    # Find specifically the S_Hora device
    hora_device = equipment.find(".//ns:Device[@id='S_Hora']", ns)
    
    print(f"Device ID: {hora_device.get('id')}")
    print(f"Name: {hora_device.find('ns:Name', ns).text}")
    print(f"Description: {hora_device.find('ns:Description', ns).text}")
    print(f"Type: {hora_device.find('ns:Type', ns).text}")
    
    # Get DataPoints for S_Hora
    datapoints = hora_device.find('ns:DataPoints', ns)
    print("\nDataPoints:")
    for datapoint in datapoints.findall('ns:DataPoint', ns):
        print(f"\t- ID: {datapoint.get('id')}")
        print(f"\t  Name: {datapoint.find('ns:Name', ns).text}")
        print(f"\t  DataType: {datapoint.find('ns:DataType', ns).text}")
        print(f"\t  Unit: {datapoint.find('ns:Unit', ns).text}")
        print(f"\t  Value: {datapoint.find('ns:Value', ns).text}")
        print("-------------------")
    try:
        server = Server_hora()
        server.run()
    except Exception as e:
        print(f"An error occurred: {e}")    
    finally:
        if server:
            server.server.stop()
            print("Server stopped.")    

