import time
from asyncua.sync import Server
from asyncua import ua
import json
import datetime
import xml.etree.ElementTree as ET

class Base_Server:
    def __init__(self, endpoint_port, server_name):
        self.server = Server()
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        self.server.set_server_name(f"OPC UA {server_name}")
        self.server.set_endpoint(f"opc.tcp://DESKTOP-M1F986I:{endpoint_port}/OPCUA/{server_name}")
        self.server.set_security_IDs(["Anonymous"])
        self.uri = "http://www.epsa.upv.es/entornos/NJFJ"
        self.idx = self.server.register_namespace(self.uri)
        print(f'nuestro idx: {self.idx}')
        
        # Import the XML model before creating nodes
        self.import_xml_model()
        
        # Add objects folder
        self.objects = self.server.nodes.objects
        # Create device folder
        self.devices_folder = self.objects.add_folder(self.idx, "Devices")
        
    def import_xml_model(self):
        try:
            self.server.import_xml("Modelo_datos.xml")
            print("XML model imported successfully")
        except Exception as e:
            print(f"Error importing XML model: {e}")

    def load_model(self):
        tree = ET.parse('Modelo_datos.xml')
        return tree.getroot()

    def print_type_structure(self, root, type_id):
        # ... (keep existing print_type_structure method)
        pass

    def load_model_and_print_structure(self):
        root = self.load_model()
        print("\n=== Server Types Structure ===")
        self.print_type_structure(root, self.server_type) 