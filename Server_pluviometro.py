import time
from asyncua.sync import Server
from asyncua import ua
import json
import datetime
import math as mt
import xml.etree.ElementTree as ET

class Server_pluviometro:
    def __init__(self):
        self.server = Server()
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        self.server.set_server_name("OPC UA Pluviometro Server")
        self.server.set_endpoint("opc.tcp://DESKTOP-M1F986I:5380/OPCUA/PluviometroServer")
        #self.server.set_endpoint("opc.tcp://LAPTOP-PIE5PVF8:53540/OPCUA/AforoServer")
        self.server.set_security_IDs(["Anonymous"])
        self.uri = "http://www.epsa.upv.es/entornos/NJFJ/Pluviometro"
        self.idx = self.server.register_namespace(self.uri)
        print(f'nuestro idx: {self.idx}')
        time.sleep(1)
        self.timestamps = self.process_json_data("chiva.json")
        # Add objects folder
        self.objects = self.server.nodes.objects
        
        # Create device folder
        self.devices_folder = self.objects.add_folder(self.idx, "Devices")



    def run(self):
        
        self.server.start()
        print("Server started successfully!")
        self.hora, self.pluviometro_5min, self.pluviometro_1h, self.estado = self.setupVariables()
        self.server_running = True

        for data in self.timestamps:
            if self.server_running:
                self.send_data(data)
                time.sleep(1.0)
            else:
                break
    def process_json_data(self, json_file_path):
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        
        processed_data = []
        for reading in data:
            print(f"Processing: {reading['FechaHora']}")
            timestamp = datetime.datetime.strptime(reading['FechaHora'], '%Y-%m-%dT%H:%M:%S')
            if reading['Estado'] == True:
                pluviometro_value = float(reading['Pluvimetro (mm)'])
            else:
                nan_value = float('nan')
                pluviometro_value = nan_value
            processed_data.append({
                'timestamp': timestamp,
                'pluviometro': pluviometro_value
            })
        return processed_data

    def setupVariables(self):
        root = self.load_model()
        
        ns = {'ns': 'http://opcfoundation.org/UA/2011/03/UANodeSet.xsd'}
        
        # Get Pluviometro device from XML
        equipment = root.find('.//ns:Equipment', ns)
        
        pluviometro_device = equipment.find(".//ns:Device[@id='S_Pluviometro']", ns)
        
        # Create folder structure
        device_name = pluviometro_device.find('ns:Name', ns).text
        print("1. Device name:", device_name)
        
        folder_pluviometro = self.devices_folder.add_folder(self.idx, device_name)
        
        
        # Create the object
        device_description = pluviometro_device.find('ns:Description', ns).text
        
        objPluviometro = folder_pluviometro.add_object(self.idx, device_description)
        
        # Add variables based on XML DataPoints
        datapoints = pluviometro_device.find('ns:DataPoints', ns)
        
        # Create Pluviometro variable
        pluviometro_dp = datapoints.find(".//ns:DataPoint[@id='Pluviometro_5min']", ns)
        
        pluviometro_5min = objPluviometro.add_variable(
            self.idx,
            pluviometro_dp.find('ns:Name', ns).text,
            float(pluviometro_dp.find('ns:Value', ns).text),
            ua.VariantType.Float
        )
        print("2. Pluviometro_5min variable created")
        
        pluviometro_dp = datapoints.find(".//ns:DataPoint[@id='Pluviometro_1h']", ns)
        
        pluviometro_1h = objPluviometro.add_variable(
            self.idx,
            pluviometro_dp.find('ns:Name', ns).text,
            float(pluviometro_dp.find('ns:Value', ns).text),
            ua.VariantType.Float
        )
        print("3. Pluviometro_1h variable created")
        
        # Create Estado variable
        estado_dp = datapoints.find(".//ns:DataPoint[@id='Pluviometro_Estado']", ns)
        print("4. Estado found:", estado_dp is not None)
        
        estado = objPluviometro.add_variable(
            self.idx,
            estado_dp.find('ns:Name', ns).text,
            estado_dp.find('ns:Value', ns).text.lower() == 'true',
            ua.VariantType.Boolean
        )
        print("5. Estado variable created")
        
        # Create Hora variable
        hora_dp = datapoints.find(".//ns:DataPoint[@id='Hora_Pluviometro']", ns)
        
        hora = objPluviometro.add_variable(
            self.idx,
            hora_dp.find('ns:Name', ns).text,
            self.timestamps[0]['timestamp'],
            ua.VariantType.DateTime
        )
        print("6. Hora variable created")
        pluviometro_5min.set_writable()
        pluviometro_1h.set_writable()
        estado.set_writable()
        hora.set_writable()
        return hora, pluviometro_5min, pluviometro_1h, estado

    

    def send_data(self, data):
        contador = 0
        print(f"Timestamp: {data['timestamp']}, Pluviometro: {data['pluviometro']}")
        self.hora.write_value(data['timestamp'])
        self.pluviometro_5min.write_value(ua.Float(data['pluviometro']))
        
        self.pluviometro_1h.write_value(ua.Float(data['pluviometro']))
        
        # Estado could be updated based on some condition if needed

    def load_model(self):
        tree = ET.parse('Modelo_datos.xml')
        return tree.getroot()

if __name__ == "__main__":
    print("-------------------")
    try: 
        server = Server_pluviometro()
        root = server.load_model()
        print(root)
        
        # Define the namespace
        ns = {'ns': 'http://opcfoundation.org/UA/2011/03/UANodeSet.xsd'}
        
        # Get Equipment node with namespace
        equipment = root.find('.//ns:Equipment', ns)
        
        # Find specifically the S_Pluviometro device
        pluviometro_device = equipment.find(".//ns:Device[@id='S_Pluviometro']", ns)
        
        print(f"Device ID: {pluviometro_device.get('id')}")
        print(f"Name: {pluviometro_device.find('ns:Name', ns).text}")
        print(f"Description: {pluviometro_device.find('ns:Description', ns).text}")
        print(f"Type: {pluviometro_device.find('ns:Type', ns).text}")
        
        # Get DataPoints for Aforo
        datapoints = pluviometro_device.find('ns:DataPoints', ns)
        print("\nDataPoints:")
        for datapoint in datapoints.findall('ns:DataPoint', ns):
            print(f"\t- ID: {datapoint.get('id')}")
            print(f"\t  Name: {datapoint.find('ns:Name', ns).text}")
            print(f"\t  DataType: {datapoint.find('ns:DataType', ns).text}")
            print(f"\t  Unit: {datapoint.find('ns:Unit', ns).text}")
            print(f"\t  Value: {datapoint.find('ns:Value', ns).text}")
            print("-------------------")
        
        # Start the server
        server.run()
        
        # Keep the server running
        while True:
            time.sleep(1)
            
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'server' in locals():
            server.server.stop()
            print("Server stopped.")
    
