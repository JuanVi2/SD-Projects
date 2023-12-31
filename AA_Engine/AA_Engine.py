import hashlib
import signal
import sqlite3
from time import sleep
from json import dumps, loads
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from sys import argv
from kafka.admin import NewTopic
import time
import random
import threading
import socket
import json
import os
import traceback
import requests
import bcrypt
import kafka

def leer_matriz_desde_archivo(nombre_archivo):
    matriz = []
    with open(nombre_archivo, 'r') as archivo:
        for linea in archivo:
            fila = [valor.strip().replace('B', ' ') 
                    for valor in linea.strip().split(' ')]
            matriz.append(fila)
    return matriz

def signal_handler(sig, frame):
    """
    Maneja la flag de final para terminal el bucle infinito cuando se le manda SIGINT
    """
    global running
    print("TERMINANDO PROCESO DE ENGINE")
    running = False
    exit()

def limpiar_posicion(mapa, alias):
    for i in range(len(mapa)):
        for j in range(len(mapa[i])):
            if(mapa[i][j] == alias):
                mapa[i][j] = ' '
    return mapa

def pelea_npc(enemigo, jugador):
    nivel_jugador = obtener_poder(jugador)
    nivel_enemigo = enemigo['nivel']
    
    if(nivel_enemigo < nivel_jugador):
        MAPA[jugador['posicion'][0], jugador['posicion'][1]] = jugador['alias'].lower().strip()[0]
        enemigo['nivel'] = -1
        print("Jugador gana")
    elif(nivel_enemigo > nivel_jugador):
        jugador['nivel'] = -1
        print("Jugador muerto")

def comprobar_pelea(enemigo, jugador):
    nivel_jugador = obtener_poder(jugador)
    nivel_enemigo = obtener_poder(enemigo)
    if(nivel_enemigo < nivel_jugador):
        MAPA[jugador['posicion'][0], jugador['posicion'][1]] = jugador['alias'].lower().strip()[0]
        enemigo['nivel'] = -1
        print("Jugador gana")
    elif(nivel_enemigo > nivel_jugador):
        jugador['nivel'] = -1
        print("Jugador muerto")


def incluir_npc(mapa, npc):
    mapa = limpiar_posicion(mapa, npc['nivel'])
    posicion = npc['posicion']
    if(posicion == None):
        posicion = [random.randint(0, 19), random.randint(0, 19)]
    else:
        mapa[posicion[0]][posicion[1]] = npc['nivel']
    return mapa

def comprobar_temperatura(posicion):
    calor = -1
    if(posicion[0] < 10):
        if(posicion[1] < 10):
            if(TEMPERATURAS[0].split(',')[1] <= 10):
                calor = False
            elif(TEMPERATURAS[0].split(',')[1] >= 25):
                calor = True
        else:
            if(TEMPERATURAS[2].split(',')[1] <= 10):
                calor = False
            elif(TEMPERATURAS[2].split(',')[1] >= 25):
                calor = True
    else:
        if(posicion[1] < 10):
            if(TEMPERATURAS[1].split(',')[1] <= 10):
                calor = False
            elif(TEMPERATURAS[1].split(',')[1] >= 25):
                calor = True
        else:
            if(TEMPERATURAS[3].split(',')[1] <= 10):
                calor = False
            elif(TEMPERATURAS[3].split(',')[1] >= 25):
                calor = True
    
    return calor

def obtener_poder(jugador):
    calor = comprobar_temperatura(jugador['posicion'])
    if(calor == -1):
        return jugador['nivel']
    elif(calor):
        return jugador['nivel'] + jugador['ec']
    else:
        return jugador['nivel'] + jugador['ef']

def incluir_jugadores(mapa, jugador):
    global MAPA
    mapa = limpiar_posicion(mapa, jugador['alias'].lower().strip()[0])
    posicion = jugador['posicion']
    if(posicion == None):
        posicion = [random.randint(0, 19), random.randint(0, 19)]
        jugador['posicion'] = posicion
    if(mapa[posicion[0]][posicion[1]] == 'A'):
        #Aqui debo subirle un nivel al jugador
        mapa[posicion[0]][posicion[1]] = jugador['alias'].lower().strip()[0]
        print(jugador['alias'] + " sube de nivel")
        jugador['nivel'] = jugador['nivel'] + 1
    elif(mapa[posicion[0]][posicion[1]] == 'M'):
        #Aqui debo matar al jugador que será pasarle el nivel a -1
        jugador['nivel'] = -1
        mapa[posicion[0]][posicion[1]] = ' '
    elif(mapa[posicion[0]][posicion[1]] == ' '):
        #Simplemente se mueve el jugador
        mapa[posicion[0]][posicion[1]] = jugador['alias'].lower().strip()[0]
    else:
        if(mapa[posicion[0]][posicion[1]].isdigit()):
            
            print("NPC encontrado")
            for i in NPCS:
                if(i['nivel'] == mapa[posicion[0]][posicion[1]]):
                    enemigo = i
            pelea_npc(enemigo, jugador)
        else:
            print("Jugador enemigo encontrado")
            enemigo = 0
            for j in JUGADORES:
                if(j['alias'].lower().strip()[0] == mapa[posicion[0]][posicion[1]]):
                    enemigo = j

            comprobar_pelea(enemigo, jugador)

            

    MAPA = mapa
    return jugador

class TiemposMapa(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.ciudades = "ciudades.txt"
        self.api_key_file = "./api_key"

    def stop(self):
        self.stop_event.set()

    def setApiKey(self):
        with open(self.api_key_file, "r") as f:
            self.api_key = f.readline().strip()
            f.close()


    def getCiudades(self):
        ciudades = []
        f = open(self.ciudades, "r")
        l = f.readline().strip()
        while l != "":
            ciudades.append(l)
            l = f.readline().strip()
        f.close()
        return ciudades
    
    def elegirCiudades(self):
        ciudades = self.getCiudades()
        ciudades_aleatorias = random.sample(ciudades, 4)
        print("Las ciudades elegidas son las siguientes: ")
        print(ciudades_aleatorias)
        return ciudades_aleatorias


    def consumir(self) -> None:
        url = "https://api.openweathermap.org/data/2.5/weather"
        ciudades = self.elegirCiudades()
        global TEMPERATURAS
        for ciudad in ciudades:
            time.sleep(1)
            try:
                params = {'q': ciudad, 'appid': self.api_key}
                print(url)
                fu = f"https://api.openweathermap.org/data/2.5/weather?q={ciudad}\&appid={self.api_key}"
                print(params)
                response = requests.get(url, params=params)
                if response.status_code == 200:
                    respuesta = response.json()
                    temperatura = respuesta["main"]["temp"] - 273.15
                    TEMPERATURAS.append(temperatura)
                    
                    print(f"Temperatura de {ciudad} es {temperatura}", temperatura)
                else:
                    print(f"Error al recuperar tiempos {ciudad}")
            except Exception as e:
                print("ERROR al obtener la temperatura", e)
                traceback.print_exc()
        print("Temperaturas obtenidas")
        print(TEMPERATURAS)
            

    def run(self):
        print("Obteniendo temperaturas")
        try:
            self.setApiKey()
            while (not self.stop_event.is_set()):
                self.consumir()
                if(len(TEMPERATURAS) == 4):
                    self.stop_event.set()
        except Exception as e:
            print("ERROR al obtener las temperaturas :", e)
            traceback.print_exc()
        finally:
            print("Final de servidor de temperatura")
         
class Partida(threading.Thread):

    def __init__(self, ip, port, topic, alias):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.ip = ip
        self.port = port
        self.topic = topic
        self.alias = alias

    def stop(self):
        self.stop_event.set()

    def encontrarJugador(self, alias):
        for jugador in JUGADORES:
            if(jugador['alias'] == alias):
                return jugador

    def consumir(self, consumer, producer):
        global MAPA
        global JUGADORES
        while (not self.stop_event.is_set()) and running:
            
            for msg in consumer:
                print(self.topic, " : ", msg.value)
                print(JUGADORES)
                try:
                    if msg.value['msg'] == 'no' or not running:
                        JUGADORES = [item for item in JUGADORES if item.get('nivel') != -1]
                        print("JUGADOR MUERTO: ",self.alias)
                        self.stop_event.set()
                        self.stop()
                        return
                    if(msg.value['posicion'] == []):
                        for jugador in JUGADORES:
                            if(jugador['alias'] == self.alias):
                                data = {
                                    'mapa': MAPA,
                                    'posicion': jugador['posicion'],
                                    'nivel': jugador['nivel']
                                }
                                break
                    else:
                        for jugador in JUGADORES:
                            if(jugador['alias'] == self.alias):
                                jugador['posicion'] = msg.value['posicion']
                                incluir_jugadores(MAPA, jugador)
                                data = {
                                    'mapa': MAPA,
                                    'posicion': msg.value['posicion'],
                                    'nivel': jugador['nivel']
                                }
                                break

                    print(f"Partida envio de mapa para {self.topic}")
                    print(JUGADORES)
                    print(data)
                    producer.send(self.topic + 'out', value=data)
                    print("enviado")
                except Exception as e:
                    print("ERROR EN Partida consumir", e)
                    traceback.print_exc()

    def run(self):
        print("INICIO PARTIDA " + self.topic)
        try:
            consumer = KafkaConsumer(self.topic + 'in',
                                     bootstrap_servers=f'{self.ip}:{self.port}',
                                     consumer_timeout_ms=100,
                                     value_deserializer=lambda x: json.loads(x.decode('utf-8')))
            
            producer = KafkaProducer(bootstrap_servers=f'{self.ip}:{self.port}',
                                     value_serializer=lambda x: 
                                        json.dumps(x).encode('utf-8'))
            self.consumir(consumer, producer)
        except Exception as e:
            print("ERROR EN LectorMovimientos :", e)
            traceback.print_exc()
        finally:
            if 'consumer' in locals():
                consumer.close()
            if 'producer' in locals():
                producer.close()
            print("FIN LectorMovimientos")


class LectorMovimientos(threading.Thread):
    def __init__(self, ip, port, database):
        self.database = database
        self.ip_kafka = ip
        self.port_kakfa = port
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.visitantes = {}
        self.posiciones = {}

    def stop(self):
        self.stop_event.set()

    def actualizaMapa(self):
        global NPCS
        global MAPA
        global JUGADORES

        mapa = MAPA
        jugadores = JUGADORES
        npcs = NPCS
        

        for j in jugadores:
            if j['nivel'] > 0:
                incluir_jugadores(mapa, j)

        for j in npcs:
            
            if j['nivel'] > 0:
                incluir_npc(mapa, j)
        MAPA = mapa

    def run(self):
        print("Recargando el mapa")
        try:
            while (not self.stop_event.is_set()) and running:
                self.actualizaMapa()
                time.sleep(1)
        except Exception as e:
            print("ERROR INTENTANDO ACTUALIZAR EL MAPA :", e)
            traceback.print_exc()
        finally:
            print("MAPA ACTUALIZADO")



class AccesManager(threading.Thread):
    """
    Clase Thread que se conecta cada 3 segundos al servidor de tiempos que le
    hayamos indicado.
    """

    def __init__(self, ip, port, database):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.ip = ip
        self.port = port
        self.database = database

    def stop(self):
        self.stop_event.set()

    def login(self, alias, passwd):
        final = False
        con = sqlite3.connect(DATABASE)
        cur = con.cursor()
        for row in cur.execute(f"select passwd from users where alias like '{alias}';"):
            bd_passwd = row[0]
    
        if bd_passwd is None:
            print("ERROR al hacer login, no existe el usuario")
            return False
    
        if bcrypt.checkpw(passwd.encode('utf-8'), bd_passwd):
            final = True
            print(f"LOGIN CON EXITO ")
        else:
            print(f"LOGIN INCORRECTO ")
        con.close()
        return final

    def obtenerPersonaje(self, alias):
        con = sqlite3.connect(DATABASE)
        cur = con.cursor()
        for row in cur.execute(f"select * from users where alias like '{alias}';"):
            personaje = row
        con.close()
        return personaje

    def createTopic(self, topic):
        consumer = kafka.KafkaConsumer(group_id='test2', bootstrap_servers=f'{self.ip}:{self.port}')
        tops = consumer.topics()
        if (topic + 'in' not in tops) and (topic + 'out' not in tops):
            admin = KafkaAdminClient(bootstrap_servers=f'{self.ip}:{self.port}')
            topic_list = [NewTopic(name=topic + 'in', num_partitions=2, replication_factor=1)]
            topic_list += [NewTopic(name=topic + 'out', num_partitions=2, replication_factor=1)]
            admin.create_topics(new_topics=topic_list, validate_only=False)

    def consumir(self, consumer, producer):
        global LIMITE
        manejadores = []
        while (not self.stop_event.is_set()) and running:
            for msg in consumer:
                if len(manejadores) >= LIMITE:
                    data = 'no'
                    producer.send("accesoout", value=data) 
                try:
                    print(msg.value)
                    alias = msg.value.split(".")[0]
                    passwd = msg.value.split(".")[1]
                    if self.login(alias, passwd) and len(manejadores) < LIMITE:
                        print(f"Login Exito {alias} {passwd}")
                        jugador = self.obtenerPersonaje(alias)
                        topic = alias+str(int(time.time()))
                        JUGADORES.append({'alias': jugador[0], 'password': jugador[1], 'posicion': None, 'nivel': jugador[2], 'ec': jugador[3], 'ef': jugador[4]})
                        self.createTopic(topic)
                        manejadores.append(Partida(self.ip, self.port, topic, jugador[0]))
                        manejadores[-1].start()
                        time.sleep(1)

                        producer.send("accesoout", topic)
                    else:
                        time.sleep(1)
                        print(f"Login Fallo {alias} {passwd}")
                        data = 'no'
                        producer.send("accesoout", data)
                except Exception as e:
                    print("ERROR EN AccesManager", e)
                    traceback.print_exc()

    def run(self):
        print("Escuchando peticiones de jugadores")
        try:
            consumer = KafkaConsumer('accesoin',
                                     bootstrap_servers=f'{self.ip}:{self.port}',
                                     consumer_timeout_ms=100,
                                     value_deserializer=lambda x: loads(x.decode('utf-8')))
            

            producer = KafkaProducer(bootstrap_servers=f'{self.ip}:{self.port}',
                                     value_serializer=lambda x: 
                                        dumps(x).encode('utf-8'))
            self.consumir(consumer, producer)
        except Exception as e:
            print("ERROR al escuchar las peticiones de los jugadores :", e)
            traceback.print_exc()
        finally:
            if 'consumer' in locals():
                consumer.close()
            print("Cerrando las peticiones de jugadores")

class ObtenerNPC(threading.Thread):

    def __init__(self, ip, port):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.ip = ip
        self.port = port

    def stop(self):
        self.stop_event.set()

    def createTopic(self, topic):
        consumer = kafka.KafkaConsumer(group_id='test', bootstrap_servers=f'{self.ip}:{self.port}')
        tops = consumer.topics()
        
        if (topic + 'in' not in tops) and (topic + 'out' not in tops):
            print("Creando topic")
            admin = KafkaAdminClient(bootstrap_servers=f'{self.ip}:{self.port}')
            topic_list = [NewTopic(name=topic + 'in', num_partitions=2, replication_factor=1)]
            topic_list += [NewTopic(name=topic + 'out', num_partitions=2, replication_factor=1)]
            admin.create_topics(new_topics=topic_list, validate_only=False)   

    def consumir(self, consumer, producer):
        while (not self.stop_event.is_set()) and running:
            
            print("Bucle de obtener npc")
            for msg in consumer:
                
                try:
                    global MAPA
                    global NPCS
                    print("Recibiendo NPC")
                    npc = {}
                    existe = False
                    
                    npc = msg.value['npcs'][0]
                    MAPA = incluir_npc(MAPA, msg.value['npcs'][0])

                    data = {'mapa': MAPA,
                            'id' : npc['id'],
                            'nivel' : npc['nivel']
                            }
                    

                    for i in NPCS:
                        if i['id'] == npc['id']:
                            existe = True
                            i['posicion'] = npc['posicion']
                            print("El npc se esta moviendo")
                            break
                    if(existe != True):
                        print("Npc nuevo")
                        topic = npc['id']
                        self.createTopic('NPC' + topic + 'nuevo')
                        print("El topic creado es el siguiente: " + topic)
                        NPCS.append(npc)
                    
                    sleep(2)
                    print(NPCS)
                    sleep(2)
                    print(MAPA)
                    
                    producer.send(npc['id'] + 'in', value = data)
                    
                except Exception as e:
                    print("Error al solicitar los NPCs:", e)
                    traceback.print_exc()
                
    def run(self):
        
            try:
                producer = KafkaProducer(bootstrap_servers=f'{self.ip}:{self.port}',
                                        value_serializer=lambda x: 
                                        dumps(x).encode('utf-8'))
                
                consumer = KafkaConsumer('Npc',
                                    bootstrap_servers=f'{self.ip}:{self.port}',
                                    enable_auto_commit=True,
                                    group_id='Npc-group',
                                    value_deserializer=lambda x: loads(x.decode('utf-8')))
                
                print("Insertando npcs")
                
                self.consumir(consumer,producer)

                
                print(len(NPCS))
            except Exception as e:
                print("Error al solicitar los NPCS:", e)
                traceback.print_exc()
            finally:
                if 'consumer' in locals():
                    consumer.close()
                print("Cerrando conexión con el servidor de NPCS")
                
class GuardarMapa(threading.Thread):
    
    def __init__(self, database_path):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.database_path = "./mapa_juego.txt"

    def stop(self):
        self.stop_event.set()

    def run(self):
        print("Guardando el mapa en la base de datos")
        try:
            while (not self.stop_event.is_set()) :
                time.sleep(3)
                with open(self.database_path, 'w') as f:
                    f.write(json.dumps(MAPA))
                # print("storeMap guardo el mapa en la base de datos")
        except Exception as e:
            print("ERROR GUARDANDO EL MAPA EN BBDD :", e)
            traceback.print_exc()

        finally:
            print("Mapa guardado en la base de datos")

def handle_client(conn, addr):
    """
    :param conn: conexion con el cliente
    :param addr: direccion del cliente
    """
    HEADER = 10
    resultado = False
    print(f"NUEVA CONEXION: {addr}")

    c_length = int(conn.recv(HEADER))
    credentials = conn.recv(c_length).decode()

    print(f"recibido: {credentials}")
    modo = credentials.split(":")[0]

    

    conn.send(b'ok')
    conn.close()

def repartidor(server):

    server.listen()
    print(f"ESCUCHANDO EN {IP_SOCKET}:{PORT_SOCKET}")
    num_conexiones = threading.active_count() - 1
    print(f"N CONEXIONES ACTUAL: {num_conexiones}")
    
    while True:
        conn, addr = server.accept()
        num_conexiones = threading.active_count()
        if num_conexiones >= LIMITE:
            print("LIMITE DE CONEXIONES EXCEDIDO")
            conn.send(b"EL SERVIDOR HA EXCEDIDO EL LIMITE DE CONEXIONES")
            conn.close()
            num_conexiones = threading.active_count() - 1
        else:
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {num_conexiones}")
            print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", LIMITE - num_conexiones)

def handle_socket_requests():
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind((IP_SOCKET, PORT_SOCKET))
    try:
        repartidor(serversocket)
    except Exception as e:
        print("ERROR: ", e)
    finally:
        serversocket.close()

if __name__ == "__main__":


    uso= "python AA_Engine.py <ip_espera:puerto> <numero maximo de jugadores> <ip_kafka:puerto> <bbdd>"
    if len(argv) < 5:
        print(uso)
        exit(0)

    running = True
    nombre_archivo = 'mapa.txt'
    tiempo_juego = 100
    tiempo_ïnicial = time.time()
    JUGADORES = []
    PORT_SOCKET = int(argv[1].split(':')[1])
    IP_SOCKET = argv[1].split(':')[0]
    TEMPERATURAS = []
    NPCS = []
    LIMITE = int(argv[2])
    print(argv)
    DATABASE = argv[4]
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    cur.execute("SELECT * FROM users")
    rows = cur.fetchall()
    for row in rows:
        print(row)
    con.close()

    signal.signal(signal.SIGINT, signal_handler)

    clases = [
        LectorMovimientos(argv[3].split(':')[0],
                          int(argv[3].split(':')[1]),
                          argv[4]),
        GuardarMapa(argv[4]),
        TiemposMapa(),
        ObtenerNPC(argv[3].split(':')[0],
                        int(argv[3].split(':')[1])),
        AccesManager(argv[3].split(':')[0],
                     int(argv[3].split(':')[1]),
                     argv[4]) 

    ]
    
    socket_thread = threading.Thread(target=handle_socket_requests)
    # Esto lee el mapa inicial con las minas y los alimentos
    matriz_inicial = leer_matriz_desde_archivo(nombre_archivo)
    MAPA = matriz_inicial
    for c in clases:
            c.setDaemon(True)

    for c in clases:
        c.start()
    os.system("python ./Api_Engine.py")

    while running and time.time() - tiempo_ïnicial < tiempo_juego:
        pass
        
        

    print("Ha terminado el tiempo de juego")  
    time.sleep(1)

    for c in clases:
        c.stop()

    for c in clases:
        c.join()

    nivel = 0
    alias = ""
    for jugador in JUGADORES:
        if(jugador['nivel'] > nivel):
            nivel = jugador['nivel']
            alias = jugador['alias']
    if(nivel != 0):
        print("El ganador es: " + alias + " con nivel: " + str(nivel))
    else:
        print("No hay ganador")    
    exit(0)
    

   
