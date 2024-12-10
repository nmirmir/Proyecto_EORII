import time
from asyncua.sync import Client
from asyncua import ua
estado_alerta_caudal = False
estado_alerta_pluvimetro = False


class OPCUAClient:
    def __init__(self, server_url, variable_name):
        self.client = Client(server_url)
        self.variable_name = variable_name
#conexion
    def connect(self):
        self.client.connect()
        print(f"Connected to {self.client.server_url}")

    def disconnect(self):
        self.client.disconnect()
        print(f"Disconnected from {self.client.server_url}")

    def read_variable(self):
        root = self.client.nodes.root
        objects = root.get_child(["0:Objects"])
        variable = objects.get_child([f"2:{self.variable_name}"])
        value = variable.get_value()
        return value


if __name__ == "__main__":
    # URLs of the servers
    server_hora_url = "opc.tcp://localhost:5350/OPCUA/SimulationServer"
    server_caudal_url = "opc.tcp://localhost:5360/OPCUA/SimulationServer"
    server_pluvimetro_url = "opc.tcp://localhost:5380/OPCUA/PluviometroServer"

    # Names of the variables
    variable_hora = "Devices/Horario/FechaHora"
    variable_caudal = "Devices/Caudal/Caudal"
    variable_pluvimetro_5min = "Devices/Pluviometro/Pluviometro_5min"
    variable_pluvimetro_1h = "Devices/Pluviometro/Pluviometro_1h"

    # Create clients
    client_hora = OPCUAClient(server_hora_url, variable_hora)
    client_caudal = OPCUAClient(server_caudal_url, variable_caudal)
    client_pluvimetro_5min = OPCUAClient(server_pluvimetro_url, variable_pluvimetro_5min)
    client_pluvimetro_1h = OPCUAClient(server_pluvimetro_url, variable_pluvimetro_1h)

    try:
        # Connect to all servers
        client_hora.connect()
        client_caudal.connect()
        client_pluvimetro_5min.connect()
        client_pluvimetro_1h.connect()

        while True:
            # Read all variables
            hora = client_hora.read_variable()
            caudal = client_caudal.read_variable()
            pluvimetro_5min = client_pluvimetro_5min.read_variable()
            pluvimetro_1h = client_pluvimetro_1h.read_variable()

            # Print all values
            print(f"Hora: {hora}")
            print(f"Caudal: {caudal} m³/s")
            print(f"Pluviometro 5min: {pluvimetro_5min} mm")
            print(f"Pluviometro 1h: {pluvimetro_1h} mm")
            print("-" * 50)
            
            time.sleep(2)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Disconnect from all servers
        client_hora.disconnect()
        client_caudal.disconnect()
        client_pluvimetro_5min.disconnect()
        client_pluvimetro_1h.disconnect()