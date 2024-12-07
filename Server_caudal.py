import time
from asyncua.sync import Server
from asyncua import ua
import json
import datetime
import math as mt
import xml.etree.ElementTree as ET

class Server_caudal:
    def __init__(self):
        self.server = Server()
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        self.server.set_server_name("OPC UA Simulation Server")
        #self.server.set_endpoint("opc.tcp://DESKTOP-M1F986I:5360/OPCUA/SimulationServer")
        self.server.set_endpoint("opc.tcp://LAPTOP-PIE5PVF8:53540/OPCUA/AforoServer")
        self.server.set_security_IDs(["Anonymous"])
        self.uri = "http://www.epsa.upv.es/entornos/NJFJ/Caudal"
        self.idx = self.server.register_namespace(self.uri)
        print(f'nuestro idx: {self.idx}')
        time.sleep(1)
        self.timestamps = self.process_json_data("poyo.json")

        # Add objects folder
        self.objects = self.server.nodes.objects
        
        # Create device folder
        self.devices_folder = self.objects.add_folder(self.idx, "Devices")



    def run(self):
        
        self.server.start()
        print("Server started successfully!")
        self.hora, self.caudal, self.estado = self.setupVariables()
        self.server_running = True

        for data in self.timestamps:
            if self.server_running:
                self.send_data(data)
                time.sleep(1.0)  # Or whatever interval you want
            else:
                break
    def process_json_data(self, json_file_path):
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        
        # We'll modify this to store both timestamps and aforo values
        processed_data = []
        for reading in data:
            timestamp = datetime.datetime.strptime(reading['FechaHora'], '%Y-%m-%dT%H:%M:%S')
            print(reading['Caudal (m3/s)'])
            if reading['Estado'] == True:
                caudal_value = float(reading['Caudal (m3/s)'])  # Assuming this is the field name
            else:
                nan_value = float('nan')
                caudal_value = nan_value
            processed_data.append({
                'timestamp': timestamp,
                'caudal': caudal_value
            })
        return processed_data

    def setupVariables(self):
        root = self.load_model()
        ns = {'ns': 'http://opcfoundation.org/UA/2011/03/UANodeSet.xsd'}
        
        # Get Aforo device from XML
        equipment = root.find('.//ns:Equipment', ns)
        caudal_device = equipment.find(".//ns:Device[@id='S_Caudal']", ns)
        
        # Create folder structure
        device_name = caudal_device.find('ns:Name', ns).text
        folder_caudal = self.server.nodes.objects.add_folder(self.idx, device_name)
        
        # Create the object
        device_description = caudal_device.find('ns:Description', ns).text
        objCaudal = folder_caudal.add_object(self.idx, device_description)
        
        # Add variables based on XML DataPoints
        datapoints = caudal_device.find('ns:DataPoints', ns)
        
        # Create Caudal variable
        caudal_dp = datapoints.find(".//ns:DataPoint[@id='Caudal']", ns)
        caudal = objCaudal.add_variable(
            self.idx,
            caudal_dp.find('ns:Name', ns).text,
            float(caudal_dp.find('ns:Value', ns).text),
            ua.VariantType.Float
        )
        
        # Create Estado variable
        estado_dp = datapoints.find(".//ns:DataPoint[@id='Caudal_Estado']", ns)
        estado = objCaudal.add_variable(
            self.idx,
            estado_dp.find('ns:Name', ns).text,
            estado_dp.find('ns:Value', ns).text.lower() == 'true',
            ua.VariantType.Boolean
        )
        
        # Create Hora variable
        hora_dp = datapoints.find(".//ns:DataPoint[@id='Hora_Caudal']", ns)
        hora = objCaudal.add_variable(
            self.idx,
            hora_dp.find('ns:Name', ns).text,
            self.timestamps[0]['timestamp'],
            ua.VariantType.DateTime
        )
        print(f"Hora: {hora}")
        print(f"Caudal: {caudal}")
        print(f"Estado: {estado}")
        caudal.set_writable()
        estado.set_writable()
        hora.set_writable()
        return hora, caudal, estado

    

    def send_data(self, data):
        print(f"Timestamp: {data['timestamp']}, Caudal: {data['caudal']}")
        self.hora.write_value(data['timestamp'])
        self.caudal.write_value(ua.Float(data['caudal']))
        # Estado could be updated based on some condition if needed

    def load_model(self):
        tree = ET.parse('Modelo_datos.xml')
        return tree.getroot()

if __name__ == "__main__":
    print("-------------------")
    try: 
        server = Server_caudal()
        root = server.load_model()
    
        # Define the namespace
        ns = {'ns': 'http://opcfoundation.org/UA/2011/03/UANodeSet.xsd'}
       
        
        # Get Equipment node and Aforo device
        equipment = root.find('.//ns:Equipment', ns)
        caudal_device = equipment.find(".//ns:Device[@id='S_Caudal']", ns)
        
        # Add device to server
        #server.add_device(caudal_device, ns)
        
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
    
