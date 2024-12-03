import time
from asyncua.sync import Server
from asyncua import ua
import json
import datetime

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

    def run(self):
        self.server.start()
        print("Server started successfully!")

if __name__ == "__main__":
    try:    
        server = Server_caudal()
        server.run()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if server:
            server.server.stop()
            print("Server stopped.")
