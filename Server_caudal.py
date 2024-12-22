import time
from asyncua.sync import Server
from asyncua import ua
import json
import datetime
import math as mt
import xml.etree.ElementTree as ET
from asyncua.sync import Client

class SubHandler:
    def datachange_notification(self, node, val, data):
        try:
            print(f"Received updated timestamp: {val}")
            server.send_data(val)  # We'll need to make server a global variable
        except Exception as e:
            print(f"Error in callback: {e}")

    def event_notification(self, event):
        print("Python: New event", event)

class Server_caudal:
    def __init__(self):
        self.server = Server()
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        self.server.set_server_name("OPC UA Caudal Server")
        self.server.set_endpoint("opc.tcp://0.0.0.0:53530/OPCUA/ServerCaudal")
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

        # Update hora server endpoint to use 0.0.0.0
        self.hora_server_endpoint = "opc.tcp://127.0.0.1:53540/OPCUA/ServerHora"

    def run(self):
        self.server.start()
        print("Server started successfully!")
        self.hora, self.caudal, self.estado = self.setupVariables()
        self.server_running = True

        # Modified connection logic
        while self.server_running:
            try:
                if not hasattr(self, 'hora_client'):
                    print("Connecting to hora server...")
                    self.hora_client = Client(self.hora_server_endpoint)
                    self.hora_client.connect()
                    print("Connected to hora server")
                    
                    handler = SubHandler()
                    subscription = self.hora_client.create_subscription(500, handler)
                    hora_node = self.hora_client.get_node("ns=2;i=4")
                    handle = subscription.subscribe_data_change(hora_node)
                
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Connection error: {e}")
                if hasattr(self, 'hora_client'):
                    try:
                        self.hora_client.disconnect()
                    except:
                        pass
                    delattr(self, 'hora_client')
                time.sleep(5)  # Wait before retrying

    def process_json_data(self, json_file_path):
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        
        # We'll modify this to store both timestamps and aforo values
        processed_data = []
        for reading in data:
            timestamp = datetime.datetime.strptime(reading['FechaHora'], '%Y-%m-%dT%H:%M:%S')
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
        folder_caudal = self.devices_folder.add_folder(self.idx, device_name)

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
        caudal.set_writable()
        estado.set_writable()
        hora.set_writable()
        return hora, caudal, estado

    

    def send_data(self, updated_hour):
        if hasattr(updated_hour, 'tzinfo'):
            updated_hour = updated_hour.replace(tzinfo=None)
        for data in self.timestamps:
            if data['timestamp'] == updated_hour:
                print(f"Timestamp: {data['timestamp']}, Caudal: {data['caudal']}")
                self.hora.write_value(data['timestamp'])
                self.caudal.write_value(ua.Float(data['caudal']))
                break

    def load_model(self):
        tree = ET.parse('Modelo_datos.xml')
        return tree.getroot()

# Move the main execution code outside the class
if __name__ == "__main__":
    print("-------------------")
    try: 
        global server  # Make server global so SubHandler can access it
        server = Server_caudal()
        root = server.load_model()
        print(root)
        
        # Define the namespace
        ns = {'ns': 'http://opcfoundation.org/UA/2011/03/UANodeSet.xsd'}
        
        # Get Equipment node with namespace
        equipment = root.find('.//ns:Equipment', ns)
        
        # Find specifically the S_Caudal device
        caudal_device = equipment.find(".//ns:Device[@id='S_Caudal']", ns)
        
        print(f"Device ID: {caudal_device.get('id')}")
        print(f"Name: {caudal_device.find('ns:Name', ns).text}")
        print(f"Description: {caudal_device.find('ns:Description', ns).text}")
        print(f"Type: {caudal_device.find('ns:Type', ns).text}")
        
        # Get DataPoints for Aforo
        datapoints = caudal_device.find('ns:DataPoints', ns)
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
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'server' in locals():
            server.server.stop()
            print("Server stopped.")
    
