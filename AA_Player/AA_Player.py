import signal
import threading
import time
import traceback
import random
import socket
import re
import bcrypt
import requests
import ssl
import urllib3
import json
from sys import argv
import time
from kafka import KafkaConsumer, KafkaProducer

HEADER = 10

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


"""RECIBE POR LA LINEA DE PARAMETROS:
IP y puerto del AA_Engine
IP y puerto del Bootstrap Server
IP y puerto del AA_Registry"""

class ManageMovimiento(threading.Thread):
    def __init__(self, ip, port, alias, topic):
        self.ip_kafka = ip
        self.port_kakfa = port
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.mapa = []
        self.pos = [0, 0]  # posicion actual del player
        self.alias = alias
        self.topic = topic

    def stop(self):
        self.stop_event.set()   

    def decideMovimiento(self, consumidor):
        if self.objetivo[0] == self.pos[0] and self.objetivo[1] == self.pos[1]:
            self.objetivoAlcanzado(consumidor)

        direc = ['NW', 'NN', 'NE', 'WW', 'EE', 'SW', 'SS', 'SE']
        direc_1 = []
        self.eligeObjetivo()
        # print("al inicio: ",self.pos)
        nuevas = []
        dist = []
        aux = -1
        for i in range(-1, 2):
            for j in range(-1, 2):
                if not (i == j and i == 0):
                    aux += 1
                    n = [self.pos[0] + i, self.pos[1] + j]
                    if not (n[0] < 0 or n[1] < 0):
                        d = ((self.objetivo[0] - n[0]) ** 2 + (self.objetivo[1] - n[1]) ** 2) ** (1 / 2)
                        nuevas.append(n)
                        dist.append(d)
                        direc_1.append(direc[aux])

        indice = dist.index(min(dist))
        self.pos = nuevas[indice]
        # print("al final: ",self.pos)
        # print("n: ",nuevas)
        # print("d: ",dist)
        # print("D:",((self.objetivo[0] - n[0])**2 + (self.objetivo[1] - n[1])**2)**(1/2))
        # self.pos = random.choice(nuevas)
        return str(self.pos[0] * 20 + self.pos[1])
        # return 'alyx:'+random.choice(['NN', 'SS', 'EE', 'WW'])


    def actualizar_posicion(self):
    # Mostrar el mapa y la posición actual
        
        print('Posición actual:', self.pos)
        direc = ['NW', 'NN', 'NE', 'WW', 'EE', 'SW', 'SS', 'SE']
        # Solicitar entrada por teclado al jugador para actualizar la posición
        time.sleep(5)
        movimiento = random.choice(direc)

        # Actualizar la posición según el movimiento ingresado
        if movimiento == "NW":
            self.pos[0] -= 1
            self.pos[1] -= 1
        elif movimiento == "NN":
            self.pos[0] -= 1
        elif movimiento == "NE":
            self.pos[0] -= 1
            self.pos[1] += 1
        elif movimiento == "WW":
            self.pos[1] -= 1
        elif movimiento == "EE":
            self.pos[1] += 1
        elif movimiento == "SW":
            self.pos[0] += 1
            self.pos[1] -= 1
        elif movimiento == "SS":
            self.pos[0] += 1
        elif movimiento == "SE":
            self.pos[0] += 1
            self.pos[1] += 1
        else:
            print('Movimiento inválido')

        if(self.pos[0] == 20):
            self.pos[0] = 0
        elif(self.pos[0] == -1):
            self.pos[0] = 19

        if(self.pos[1] == 20):
            self.pos[1] = 0
        elif(self.pos[1] == -1):
            self.pos[1] = 19

        posicion = [self.pos[0], self.pos[1]]
        print("posicion elegida: ", posicion)
        return posicion


    def consumir(self, consumer, producer):
        global running
        print("CONSUMIENDO")
        time.sleep(3)
        print(str(0))
        data = {'msg': str(0), 'posicion': []}
        producer.send(self.topic + 'in', value=data)
        print(self.topic + 'out')
        while (not self.stop_event.is_set()) and running:
            
            for msg in consumer:
                print("entro")
                print("running?: ", running, " ", consumer.subscription())
                if not running:
                    mens = b'no'
                    producer.send(self.topic + 'in', value=mens)
                    print("envio final: ", mens)
                    self.stop()
                    return
                else:
                    
                    data = msg.value
                    
                    self.mapa = data['mapa']
                    self.pos = data['posicion']
                    if(data['nivel'] == -1):
                        print("FIN DEL JUEGO")
                        print("Has muerto")
                        running = False
                        break
                    
                    print('Mapa:')
                    print(self.mapa)
                    envio = self.actualizar_posicion()
                   
                    print("enviando: ", envio)
                    data = {'msg': str(0), 'posicion': envio}
                    producer.send(self.topic + 'in', value=data)
                    time.sleep(1)
        data = {'msg': 'no', 'posicion': []}
        producer.send(self.topic + 'in', value=data)

    def run(self):
        print("INICIO ManageMovimiento")
        try:

            producer = KafkaProducer(bootstrap_servers=f'{self.ip_kafka}:{self.port_kakfa}',
                                     value_serializer=lambda x: 
                                        json.dumps(x).encode('utf-8'))
            topic_name = self.topic + 'out'

            consumer = KafkaConsumer(topic_name,
                                    bootstrap_servers=f'{self.ip_kafka}:{self.port_kakfa}',
                                    auto_offset_reset='earliest',
                                    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
            
            print(topic_name)
            
            
            self.consumir(consumer, producer)
            print("FINAL MOVIMIENTO")
            return
        except Exception as e:
            print("ERROR EN ManageMovimiento :", e)
            traceback.print_exc()
        finally:
            if 'consumer' in locals():
                consumer.close()
            if 'producer' in locals():
                producer.close()
            print("FIN ManageMovimiento")
            exit(0)


def signal_handler(sig, frame):
    """
    Maneja la flag de final para terminal el bucle infinito cuando se le manda SIGINT
    """
    global running
    print("TERMINANDO PROCESO DE PLAYER")
    running = False
    exit(0)

def login():
    alias = input("alias: ")
    passwd = input("passwd: ")
    data = (alias + '.' + passwd)
    mensaje = data
    print("mensaje: ", mensaje)
    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print("Conectado con socket")

    try:
        consumer = KafkaConsumer('accesoout',
                                 bootstrap_servers=f'{ip_k}:{port_k}',
                                 consumer_timeout_ms=100,
                                 value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        
        producer = KafkaProducer(bootstrap_servers=f'{ip_k}:{port_k}',
                                 value_serializer=lambda x: 
                                        json.dumps(x).encode('utf-8'))
        
        producer.send('accesoin', value=mensaje)
        print("enviado al topic accesoin: ", mensaje)
        control = True
        while control:
            for msg in consumer:
                print(f"f: {msg.value}")
                if alias in msg.value:
                    print("Login Exitoso: ", msg.value)
                    a = ManageMovimiento(ip_k, port_k, alias, msg.value)
                    print("topic enviado: ", msg.value)
                    a.start()
                    control = False
                else:
                    print("Credenciales incorrectas  /  lobby lleno ")
                    control = False

    except Exception as e:
        print("ERROR EN Login :", e)
        traceback.print_exc()
    finally:
        if 'consumer' in locals():
            consumer.close()
   

def comunicacion(cliente, mensaje) -> bool:
    ret = False
    length = str(len(mensaje)).encode()
    length_msg = length + b" " * (HEADER - len(length))
    print(f"enviando: {length_msg}")
    cliente.send(length_msg)
    print(f"enviando: {mensaje.encode()}")
    cliente.send(mensaje.encode())
    respuesta = cliente.recv(2)
    if respuesta == b'ok':
        ret = True
    elif respuesta == b'no':
        ret = False
    return ret

def registra_perfil(ip, port) -> bool:
    """
        :param port: int
        :param ip: string
        :return:
        """
    alias = input("alias: ")
    passwd = input("passwd: ")
    nivel = input("nivel: ")
    ef = input("ef: ")
    ec = input("ec: ")
    credentials = f"r:{alias}:{passwd}:{nivel}:{ef}:{ec}"
    ret = False
    try:
        cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente.connect((ip, port))
        ret = comunicacion(cliente, credentials)
        if ret:
            print("PERFIL REGISTRADO CON EXITO")
        else:
            print("NO SE HA PODIDO REGISTRAR, EL USUARIO YA EXISTE")
    except Exception as e:
        print("ERROR: ", e)
    finally:
        if 'cliente' in locals():
            cliente.close()

    print("CONEXION FINALIZADA")
    return ret

def eliminar_perfil(ip, port) -> bool:
    alias = input("alias: ")
    passwd = input("passwd: ")
    credentials = f"d:{alias}:{passwd}"
    ret = False
    try:
        cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente.connect((ip, port))
        ret = comunicacion(cliente, credentials)
        if ret:
            print("PERFIL ELIMINADO CON EXITO")
        else:
            print("NO SE HA PODIDO ELIMINAR, EL USUARIO NO EXISTE")
    except Exception as e:
        print("ERROR: ", e)
    finally:
        if 'cliente' in locals():
            cliente.close()

    print("CONEXION FINALIZADA")
    return ret

def actualiza_perfil(ip, port) -> bool:
    print("CREDENCIALES ACTUALES")
    alias = input("alias: ")
    passwd = input("passwd: ")
    print("INTRODUCE LOS DATOS NUEVOS. # No rellenes los datos que no quieras cambiar")
    n_alias = input("alias: ")
    n_passwd = input("passwd: ")

    credentials = f"u:{alias}:{passwd}:{n_alias}:{n_passwd}"
    ret = False
    try:
        cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente.connect((ip, port))
        ret = comunicacion(cliente, credentials)
        if ret:
            print("PERFIL ACTUALIZADO CON EXITO")
        else:
            print("NO SE HA PODIDO ACTUALIZAR")
    except Exception as e:
        print("ERROR actualiza_perfil: ", e)
    finally:
        if 'cliente' in locals():
            cliente.close()

    print("CONEXION FINALIZADA actualiza_perfil")
    return ret


def filtra(args: list) -> bool:
    """
    Indica si el formato de los argumentos es el correcto
    :param args: Argumentos del programa
    """
    if len(args) != 4:
        print("Numero incorrecto de argumentos")
        return False

    regex_1 = '^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]{1,5}$'
    regex_2 = '^\S+:[0-9]{1,5}$'
    regex_3 = '^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]{1,5}$'
    if not (re.match(regex_1, args[1]) or re.match(regex_2, args[1]) or re.match(regex_3, args[1])):
        print("Direccion de servidor de registro incorrecta")
        return False

    if not (re.match(regex_1, args[2]) or re.match(regex_2, args[2]) or re.match(regex_3, args[2])):
        print("Direccion de servidor kafka")
        return False

    return True


def login_api():
    pass

def registro_api(alias, passwd, nivel, ef, ec):
    data = {'alias': alias, 'passwd': passwd, 'nivel': nivel, 'ef': ef, 'ec': ec}
    
    try:
        headers = {'Content-type': 'application/json'}
        r = requests.post(f"https://{ip_r}:{5055}/register", json=data, headers=headers, verify=False)
        if r.status_code == 200:
            print("PERFIL REGISTRADO CON EXITO")
        else:
            print("NO SE HA PODIDO REGISTRAR, EL USUARIO YA EXISTE")
    except Exception as e:
        print("ERROR: ", e)


def delete_api(alias, passwd):
    data = {'alias': alias, 'passwd': passwd}
    try:
        headers = {'Content-type': 'application/json'}
        r = requests.post(f"https://{ip_r}:{5055}/delete", json=data, headers=headers, verify=False)
        if r.status_code == 200:
            print("PERFIL ELIMINADO CON EXITO")
        else:
            print("NO SE HA PODIDO ELIMINAR")
    except Exception as e:
        print("ERROR: ", e)

def update_api(alias, passwd, n_alias, n_passwd):
    data = {'alias': alias, 'passwd': passwd, 'n_alias': n_alias, 'n_passwd': n_passwd}
    try:
        headers = {'Content-type': 'application/json'}
        r = requests.post(f"https://{ip_r}:{5055}/update", json=data, headers=headers, verify=False)
        if r.status_code == 200:
            print("PERFIL ACTUALIZADO CON EXITO")
        else:
            print("NO SE HA PODIDO ACTUALIZAR")
    except Exception as e:
        print("ERROR: ", e)




def pintaMenu(mode):
    print("Modo Actual: ", mode)
    print("""
    Opciones:
    m       : Modo de comunicacion (api o socket)
    l       : Inicia sesion de usuario para partida
    r       : Registra un nuevo usuario
    u       : Actualiza los datos de un usuario
    d       : Elimina un usuario
    exit    : Salir
    """)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    if not filtra(argv):
        print("ERROR: Argumentos incorrectos")
        print("Usage: AA_Player.py <ip_registro:puerto> <ip_kafka:puerto> <ip_engine:puerto>")
        print("Example: AA_Player.py 192.168.22.0:5054 192.168.22.0:9092 192.168.22.0:5053")
        exit()

    ip_r = argv[1].split(":")[0]
    port_r = int(argv[1].split(":")[1])
    ip_k = argv[2].split(":")[0]
    port_k = int(argv[2].split(":")[1])
    ip_e = argv[3].split(":")[0]
    port_e = int(argv[3].split(":")[1])

    signal.signal(signal.SIGINT, signal_handler)
    running = True
    inn = ""
    # a interactive menu to choose the action
    command = ""
    mode = 'socket'
    while command not in ['q', 'Q', 'quit', 'exit']:
        pintaMenu(mode)
        command = input("\nopcion > ")
        if command == "":
            pintaMenu(mode)
        elif command == 'm':
            aux = input("modo de comunicacion con el login (api/socket): ")
            if aux not in ['api','socket']:
                print("modo de comunicacion debe ser 'api o 'socket'")
            else:
                mode = aux
        elif command == 'l':
            if mode == 'socket':
                login()
            elif mode == 'api':
                login_api()
        elif command == 'u':
            if mode == 'socket':
                actualiza_perfil(ip_r, port_r)
            elif mode == 'api':
                print("CREDENCIALES ACTUALES")
                alias = input("alias: ")
                passwd = input("passwd: ")
                print("INTRODUCE LOS DATOS NUEVOS. # No rellenes los datos que no quieras cambiar")
                n_alias = input("alias: ")
                n_passwd = input("passwd: ")
                update_api(alias, passwd, n_alias, n_passwd)
        elif command == 'r':
            if mode == 'socket':
                registra_perfil(ip_r, port_r)
            elif mode == 'api':
                alias = input("alias: ")
                passwd = input("passwd: ")
                nivel = input("nivel: ")
                ef = input("ef: ")
                ec = input("ec: ")
                registro_api(alias, passwd, nivel, ef, ec)
        elif command == 'd':
            if mode == 'socket':
                eliminar_perfil(ip_r, port_r)
            elif mode == 'api':
                alias = input("alias: ")
                passwd = input("passwd: ")
                delete_api(alias, passwd)
        elif command == 'exit':
            running = False
        else:
            print("Comando no reconocido")




    print("FINAL")
    exit()