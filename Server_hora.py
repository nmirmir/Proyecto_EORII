import time
from asyncua.sync import Server
from asyncua import ua
import json
import datetime
import xml.etree.ElementTree as ET
multiplicador = 1

class Server_hora:
    def __init__(self):
        self.server = Server()
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        self.server.set_server_name("OPC UA Simulation Server")
        self.server.set_endpoint("opc.tcp://DESKTOP-M1F986I:53530/OPCUA/SimulationServer") #IP Nicolas
        self.server.set_security_IDs(["Anonymous"])
        self.uri = "http://www.epsa.upv.es/entornos/NJFJ"
        self.idx = self.server.register_namespace(self.uri)
        print(f'nuestro idx: {self.idx}')
        time.sleep(1)
        self.timestamps = self.process_json_timestamps("poyo.json")
        

    def run(self):
        self.server.start()
        print("Server started successfully!")
        self.fechaHora = self.setupVariables()
        self.server_running = True

        for timestamp in self.timestamps:
            if self.server_running:
                self.send_data(timestamp)
                time.sleep(1 / multiplicador)
            else:
                break   

    def process_json_timestamps(self,json_file_path):
        # Read JSON file
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        
        # Convert each date+time combination to datetime objects
        timestamps = []
        for reading in data:
            date_str = reading['fecha']
            time_str = reading['hora']
            # Combine date and time strings and convert to datetime
            datetime_str = f"{date_str} {time_str}"
            timestamp = datetime.datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            timestamps.append(timestamp)        
        return timestamps
    
    def setupVariables(self):
        # Create root folder for Horario
        folder_horario = self.server.nodes.objects.add_folder(self.idx, "Horario")
        
        # Create the object inside the folder
        objHorario = folder_horario.add_object(self.idx, "Objeto_Horario")
        
        # Add timestamp variable as an array of integers
        fechaHora = objHorario.add_variable(self.idx, "FechaHora", self.timestamps[0], ua.VariantType.DateTime)
        fechaHora.set_writable()
        
        return fechaHora

    def send_data(self,timestamp):
        self.fechaHora.write_value(timestamp)

    def load_model(self):
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
    
    # Find specifically the Aforo device
    aforo_device = equipment.find(".//ns:Device[@id='S_Aforo']", ns)
    
    print(f"Device ID: {aforo_device.get('id')}")
    print(f"Name: {aforo_device.find('ns:Name', ns).text}")
    print(f"Description: {aforo_device.find('ns:Description', ns).text}")
    print(f"Type: {aforo_device.find('ns:Type', ns).text}")
    
    # Get DataPoints for Aforo
    datapoints = aforo_device.find('ns:DataPoints', ns)
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

