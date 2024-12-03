import time
from asyncua.sync import Server
from asyncua import ua
import json
import datetime

multiplicador = 1

class Server_hora:
    def __init__(self):
        self.server = Server()
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        self.server.set_server_name("OPC UA Simulation Server")
        self.server.set_endpoint("opc.tcp://LAPTOP-PIE5PVF8:53530/OPCUA/SimulationServer") #IP Jaime
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
                time.sleep(300 / multiplicador)
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
            timestamp = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            timestamps.append(timestamp)        
        return timestamps
    
    def setupVariables(self):
        objHorario = self.server.nodes.objects.add_object(self.idx, "Objeto_Horario")
        fechaHora = objHorario.add_variable(self.idx, "FechaHora", self.timestamps[0])
        fechaHora.set_writable()
        return fechaHora

    def send_data(self,timestamp):
        self.fechaHora.write_value(timestamp)

if __name__ == "__main__":
    try:
        server = Server_hora()
        server.run()
    except Exception as e:
        print(f"An error occurred: {e}")    
    finally:
        if server:
            server.server.stop()
            print("Server stopped.")    

