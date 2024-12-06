import time
from asyncua.sync import Server
from asyncua import ua
import json
import datetime
import xml.etree.ElementTree as ET

class Server_caudal:
    def __init__(self):
        self.server = Server()
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        self.server.set_server_name("OPC UA Simulation Server")
        self.server.set_endpoint("opc.tcp://LAPTOP-QJA0AD04:53530/OPCUA/SimulationServer")
        self.server.set_security_IDs(["Anonymous"])
        self.uri = "http://www.epsa.upv.es/entornos/NJFJ/Caudal"
        self.idx = self.server.register_namespace(self.uri)
        print(f'nuestro idx: {self.idx}')
        time.sleep(1)

        # Add objects folder
        self.objects = self.server.nodes.objects
        
        # Create device folder
        self.devices_folder = self.objects.add_folder(self.idx, "Devices")
        
    def add_device(self, device_element, ns):
        """Add a device and its datapoints to the OPC UA server"""
        device_id = device_element.get('id')
        device_name = device_element.find('ns:Name', ns).text
        
        # Create device folder
        device_folder = self.devices_folder.add_folder(self.idx, device_name)
        
        # Add device properties
        device_folder.add_variable(self.idx, "Description", 
                                device_element.find('ns:Description', ns).text)
        device_folder.add_variable(self.idx, "Type", 
                                device_element.find('ns:Type', ns).text)
        
        # Add datapoints
        datapoints = device_element.find('ns:DataPoints', ns)
        for datapoint in datapoints.findall('ns:DataPoint', ns):
            dp_name = datapoint.find('ns:Name', ns).text
            dp_value = datapoint.find('ns:Value', ns).text
            dp_type = datapoint.find('ns:DataType', ns).text
            
            # Convert value based on datatype
            if dp_type == "Float":
                dp_value = float(dp_value)
            elif dp_type == "Integer":
                dp_value = int(dp_value)
            
            var = device_folder.add_variable(self.idx, dp_name, dp_value)
            var.set_writable()

    def run(self):
        self.server.start()
        print("Server started successfully!")

def load_model():
    tree = ET.parse('Modelo_datos.xml')
    return tree.getroot()

if __name__ == "__main__":
    print("-------------------")
    root = load_model()
    
    # Define the namespace
    ns = {'ns': 'http://opcfoundation.org/UA/2011/03/UANodeSet.xsd'}
    
    try:    
        server = Server_caudal()
        
        # Get Equipment node and Aforo device
        equipment = root.find('.//ns:Equipment', ns)
        aforo_device = equipment.find(".//ns:Device[@id='S_Aforo']", ns)
        
        # Add device to server
        server.add_device(aforo_device, ns)
        
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
    
