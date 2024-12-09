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
    def control_pluvimetro(hora,pluvimetro):
        maximo = 50.0  # mm/h
        if pluvimetro >=  maximo :
            estado_alerta = True
            print(f"Estado de Alerta: Activado - Hora: {hora}, Pluvimetro: {pluvimetro}")

    def control_caudal(hora , caudal):
        maximo = 100.0  # m³/s
        if caudal >= maximo:
            estado_alerta = True
            print(f"Estado de Alerta: Activado - Hora: {hora}, Caudal: {caudal}")

        

if __name__ == "__main__":
    # URLs de los servidores
    server_hora_url = "opc.tcp://DESKTOP-M1F986I:53530/OPCUA/SimulationServer"
    server_caudal_url = "opc.tcp://LAPTOP-QJA0AD04:53530/OPCUA/SimulationServer"
    #server_pluvimetro = ""

    # Nombres de las variables
    variable_hora = "Horario/Objeto_Horario/FechaHora"
    variable_caudal = "Devices/Aforo/FlowRate"  # Ajusta el nombre según tu configuración
    #variable_pluvimetro = ""

    # Crear clientes
    client_hora = OPCUAClient(server_hora_url, variable_hora)
    client_caudal = OPCUAClient(server_caudal_url, variable_caudal)
    #client_pluvimetro = OPCUAClient(server_pluvimetro_url, variable_pluvimetro)


    try:
        # Conectar a los servidores
        client_hora.connect()
        client_caudal.connect()

        while True:
            # Leer y mostrar las variables
            hora = client_hora.read_variable()
            caudal = client_caudal.read_variable()
            #pluvimetro = client_pluvimetro.read_variable()

            #monitorio por terminal de una alerta provisional
            #client_hora.control_pluvimetro(hora ,pluvimetro)
            client_caudal.control_caudal(hora ,caudal)
            print(f"Hora: {hora}, Caudal del poyo: {caudal},")
            time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Desconectar de los servidores
        client_hora.disconnect()
        client_caudal.disconnect()