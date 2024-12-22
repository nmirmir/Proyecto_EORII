import time
from asyncua.sync import Client, Server, ua
import datetime
import math

class DataCollectorServer:
    def __init__(self):
        # Initialize server
        self.server = Server()
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        self.server.set_server_name("OPC UA Data Collector Server")
        self.server.set_endpoint("opc.tcp://0.0.0.0:53570/OPCUA/DataCollector")
        self.server.set_security_IDs(["Anonymous"])
        
        # Set up namespace
        uri = "http://www.epsa.upv.es/entornos/DataCollector"
        self.idx = self.server.register_namespace(uri)
        
        # Initialize source clients with updated pluviometro port
        self.client_hora = Client("opc.tcp://127.0.0.1:53540/OPCUA/ServerHora")
        self.client_caudal = Client("opc.tcp://127.0.0.1:53530/OPCUA/ServerCaudal")
        self.client_pluviometro = Client("opc.tcp://127.0.0.1:53560/OPCUA/ServerPluviometro")
        
        # Variables to store the data
        self.hora_var = None
        self.multiplicador_var = None
        self.caudal_var = None
        self.caudal_estado_var = None
        self.pluviometro_5min_var = None
        self.pluviometro_1h_var = None
        self.pluviometro_estado_var = None
        
        # Flag to control server running
        self.running = False

    def setup_variables(self):
        # Create folder structure
        objects = self.server.nodes.objects
        devices_folder = objects.add_folder(self.idx, "Devices")
        data_folder = devices_folder.add_folder(self.idx, "CollectedData")
        
        # Create variables with correct types
        self.hora_var = data_folder.add_variable(
            self.idx, "Hora", datetime.datetime.now(), ua.VariantType.DateTime
        )
        
        self.multiplicador_var = data_folder.add_variable(
            self.idx, "Multiplicador", 1.0, ua.VariantType.Double
        )
        
        # Caudal variables
        self.caudal_var = data_folder.add_variable(
            self.idx, "Caudal", 0.0, ua.VariantType.Double
        )
        self.caudal_estado_var = data_folder.add_variable(
            self.idx, "Caudal_Estado", True, ua.VariantType.Boolean
        )
        
        # Pluviometro variables
        self.pluviometro_5min_var = data_folder.add_variable(
            self.idx, "Pluviometro_5min", 0.0, ua.VariantType.Double
        )
        self.pluviometro_1h_var = data_folder.add_variable(
            self.idx, "Pluviometro_1h", 0.0, ua.VariantType.Double
        )
        self.pluviometro_estado_var = data_folder.add_variable(
            self.idx, "Pluviometro_Estado", True, ua.VariantType.Boolean
        )
        
        # Make variables writable
        self.hora_var.set_writable()
        self.multiplicador_var.set_writable()
        self.caudal_var.set_writable()
        self.caudal_estado_var.set_writable()
        self.pluviometro_5min_var.set_writable()
        self.pluviometro_1h_var.set_writable()
        self.pluviometro_estado_var.set_writable()

    def read_source_data(self):
        try:
            # Read from hora server
            try:
                devices = self.client_hora.nodes.root.get_child(["0:Objects", "2:Devices"])
                device = devices.get_children()[0]
                obj = device.get_children()[0]
                variables = obj.get_children()
                
                hora = variables[0].get_value()
                multiplicador = float(variables[1].get_value())
                
                self.hora_var.write_value(hora)
                self.multiplicador_var.write_value(multiplicador)
            except Exception as e:
                print(f"Error reading from hora server: {e}")
                try:
                    self.client_hora.disconnect()
                    self.client_hora.connect()
                except:
                    pass
                
            # Read from caudal server
            try:
                devices = self.client_caudal.nodes.root.get_child(["0:Objects", "2:Devices"])
                device = devices.get_children()[0]
                obj = device.get_children()[0]
                variables = obj.get_children()
                
                caudal = float(variables[0].get_value())
                caudal_estado = not math.isnan(caudal)
                
                self.caudal_var.write_value(caudal)
                self.caudal_estado_var.write_value(caudal_estado)
            except Exception as e:
                print(f"Error reading from caudal server: {e}")
                try:
                    self.client_caudal.disconnect()
                    self.client_caudal.connect()
                except:
                    pass
                
            # Read from pluviometro server
            try:
                devices = self.client_pluviometro.nodes.root.get_child(["0:Objects", "2:Devices"])
                device = devices.get_children()[0]
                obj = device.get_children()[0]
                variables = obj.get_children()
                
                # Get all values from pluviometro server
                pluv_5min = float(variables[0].get_value())  # 5min value
                pluv_1h = float(variables[1].get_value())    # 1h accumulated value
                pluv_estado = variables[2].get_value()       # Estado value
                hora_pluv = variables[3].get_value()         # Hora value
                
                # Update our server variables
                self.pluviometro_5min_var.write_value(pluv_5min)
                self.pluviometro_1h_var.write_value(pluv_1h)
                self.pluviometro_estado_var.write_value(pluv_estado)
                
                # Print current values
                print(f"Hora: {hora}")
                print(f"Multiplicador: {multiplicador}")
                print(f"Caudal: {caudal} m³/s")
                print(f"Caudal Estado: {caudal_estado}")
                print(f"Pluviometro 5min: {pluv_5min} mm")
                print(f"Pluviometro 1h: {pluv_1h} mm")  # Now showing the 1h accumulated value
                print(f"Pluviometro Estado: {pluv_estado}")
                print(f"Hora Pluviometro: {hora_pluv}")
                print("-" * 50)
                
            except Exception as e:
                print(f"Error reading from pluviometro server: {e}")
                try:
                    self.client_pluviometro.disconnect()
                    self.client_pluviometro.connect()
                except:
                    pass
                
        except Exception as e:
            print(f"Error in read_source_data: {e}")

    def run(self):
        try:
            # Start server
            self.server.start()
            self.running = True
            print("Server started at opc.tcp://localhost:53570/OPCUA/DataCollector")
            
            # Set up variables first
            self.setup_variables()
            print("Variables set up")
            
            # Connect to source servers with retries
            max_retries = 3
            retry_delay = 2  # seconds
            
            for attempt in range(max_retries):
                try:
                    print(f"\nConnection attempt {attempt + 1}/{max_retries}")
                    
                    print("Connecting to hora server...")
                    self.client_hora.connect()
                    print("Connected to hora server")
                    
                    print("Connecting to caudal server...")
                    self.client_caudal.connect()
                    print("Connected to caudal server")
                    
                    print("Connecting to pluviometro server...")
                    self.client_pluviometro.connect()
                    print("Connected to pluviometro server")
                    
                    print("All servers connected successfully")
                    break
                    
                except Exception as e:
                    print(f"Connection attempt failed: {e}")
                    if attempt < max_retries - 1:
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        # Disconnect any connected clients before retrying
                        try:
                            self.client_hora.disconnect()
                        except:
                            pass
                        try:
                            self.client_caudal.disconnect()
                        except:
                            pass
                        try:
                            self.client_pluviometro.disconnect()
                        except:
                            pass
                    else:
                        raise Exception("Failed to connect to servers after all retries")
            
            # Main loop
            while self.running:
                try:
                    self.read_source_data()
                    time.sleep(2)
                except Exception as e:
                    print(f"Error in main loop: {e}")
                    if self.running:
                        print("Attempting to reconnect...")
                        try:
                            self.client_hora.disconnect()
                            self.client_caudal.disconnect()
                            self.client_pluviometro.disconnect()
                            time.sleep(1)
                            self.client_hora.connect()
                            self.client_caudal.connect()
                            self.client_pluviometro.connect()
                            print("Reconnected successfully")
                        except Exception as reconnect_error:
                            print(f"Reconnection failed: {reconnect_error}")
                        time.sleep(5)
                        
        except Exception as e:
            print(f"Server error: {e}")
            
        finally:
            self.stop()

    def stop(self):
        """Clean shutdown of the server and clients"""
        self.running = False
        try:
            # Disconnect clients
            self.client_hora.disconnect()
            self.client_caudal.disconnect()
            self.client_pluviometro.disconnect()
            print("Clients disconnected")
        except:
            pass
            
        try:
            # Stop server
            self.server.stop()
            print("Server stopped")
        except:
            pass

if __name__ == "__main__":
    server = DataCollectorServer()
    try:
        server.run()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.stop()
    except Exception as e:
        print(f"Fatal error: {e}")
        server.stop()
