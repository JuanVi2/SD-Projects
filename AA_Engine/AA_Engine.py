from time import sleep
from json import dumps, loads
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from sys import argv
import time
import random
import threading
import socket
import json
import os
import traceback

def leer_matriz_desde_archivo(nombre_archivo):
    matriz = []
    with open(nombre_archivo, 'r') as archivo:
        for linea in archivo:
            fila = [valor.strip().replace('B', ' ') 
                    for valor in linea.strip().split(' ')]
            matriz.append(fila)
    return matriz

def limpiar_posicion(matriz, alias):
    for i in range(len(matriz)):
        for j in range(len(matriz[i])):
            if(matriz[i][j] == alias):
                matriz[i][j] = ' '
    return matriz

def comprobar_pelea(nivel_enemigo, jugador):

    if(nivel_enemigo < jugador['nivel']):
        matriz[posicion[0]][posicion[1]] = jugador['alias'].lower().strip()[0]

def incluir_judarores(matriz, jugador):
    posicion = jugador['posicion']
    if(posicion == None):
        posicion = [random.randint(0, 19), random.randint(0, 19)]
    if(matriz[posicion[0]][posicion[1]] == 'A'):
        #Aqui debo subirle un nivel al jugador
        matriz[posicion[0]][posicion[1]] = jugador['alias'].lower().strip()[0]
        jugador['nivel'] = jugador['nivel'] + 1
    elif(matriz[posicion[0]][posicion[1]] == 'M'):
        #Aqui debo matar al jugador que será pasarle el nivel a -1
        jugador['nivel'] = -1
        matriz[posicion[0]][posicion[1]] = ' '
    elif(matriz[posicion[0]][posicion[1]] == ' '):
        #Simplemente se mueve el jugador
        matriz[posicion[0]][posicion[1]] = jugador['alias'].lower().strip()[0]
    else:
        comprobar_pelea(matriz[posicion[0]][posicion[1]], jugador)
    return matriz, jugador

class TiemposMapa(threading.Thread):

    def __init__(self, ip, port):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.ip = ip
        self.port = port

    def stop(self):
        self.stop_event.set()

    def consumir(self, consumer):
        for msg in consumer:
            print("Recibiendo mensaje de tiempos")
            print(msg.value)
            print(msg.value['ciudades'])
            
            TEMPERATURAS = msg.value['ciudades']
            
            print(TEMPERATURAS)
            break

    def run(self):
        while (not self.stop_event.is_set()) and running:
            try:
                producer = KafkaProducer(bootstrap_servers=f'{self.ip}:{self.port}',
                                        value_serializer=lambda x: 
                                        dumps(x).encode('utf-8'))
                data = {'Entrada': True}
                producer.send("Acceso", value=data)
                consumer = KafkaConsumer('Temperaturas',
                                    bootstrap_servers=f'{self.ip}:{self.port}',
                                    auto_offset_reset='earliest',
                                    enable_auto_commit=True,
                                    group_id='Weather-group',
                                    value_deserializer=lambda x: loads(x.decode('utf-8')))
                
                print("Insertando temperaturas")
                
                self.consumir(consumer)
                sleep(1)

                data = {'Entrada': False}
                producer.send("Acceso", value=data)
                
            except Exception as e:
                print("Error al solicitar los tiempos de las ciudades:", e)
                traceback.print_exc()
            finally:
                if 'consumer' in locals():
                    consumer.close()
                print("Cerrando conexión con el servidor de tiempos")
                break


class ObtenerNPC(threading.Thread):

    def __init__(self, ip, port):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.ip = ip
        self.port = port

    def stop(self):
        self.stop_event.set()

    def consumir(self, consumer):
        for msg in consumer:
            print("Recibiendo NPCs")
            print(msg.value)
            print(msg.value['npc'])
            
            NPCS = msg.value['npc']
            
            print(NPCS)
            break

    def run(self):
        while (not self.stop_event.is_set()) and running:
            try:
                producer = KafkaProducer(bootstrap_servers=f'{self.ip}:{self.port}',
                                        value_serializer=lambda x: 
                                        dumps(x).encode('utf-8'))
                data = {'Entrada': True}
                producer.send("Acceso", value=data)
                consumer = KafkaConsumer('Npc',
                                    bootstrap_servers=f'{self.ip}:{self.port}',
                                    auto_offset_reset='earliest',
                                    enable_auto_commit=True,
                                    group_id='Npc-group',
                                    value_deserializer=lambda x: loads(x.decode('utf-8')))
                
                print("Insertando npcs")
                
                self.consumir(consumer)
                sleep(1)

                data = {'Entrada': False}
                producer.send("Acceso", value=data)
                
            except Exception as e:
                print("Error al solicitar los NPCS:", e)
                traceback.print_exc()
            finally:
                if 'consumer' in locals():
                    consumer.close()
                print("Cerrando conexión con el servidor de NPCS")
                break



if __name__ == "__main__":


    uso= "python AA_Engine.py <ip_espera:puerto> <numero maximo de jugadores> <ip_kafka:puerto>"
    if len(argv) < 4:
        print(uso)
        exit(-1)

    running = True
    JUGADORES = {}
    TEMPERATURAS = []
    NPCS = []
    LIMITE = int(argv[2])
    print(argv)

    clases = [
        TiemposMapa(argv[3].split(':')[0],
                        int(argv[3].split(':')[1])),
        ObtenerNPC(argv[3].split(':')[0],
                        int(argv[3].split(':')[1])),

    ]

    for c in clases:
            c.setDaemon(True)

    for c in clases:
        c.start()
    #    os.system("python3 ./Api_Engine.py")

    while running:
        pass
        time.sleep(9)
        print("Terminando" )
        running = False

    print("Terminando" )  
    time.sleep(1)

    for c in clases:
        c.stop()

    for c in clases:
        c.join()

    #Variables de configuración
    nombre_archivo = 'mapa.txt'
    tiempo_juego = 1
    jugador = {
        'alias': 'Ponce',
        'password': '1234',
        'posicion': [0, 0],
        'nivel': 0,
        'ec': 0,
        'ef': 0,
    }
    posicion = [random.randint(0, 19), random.randint(0, 19)]

    # Esto lee el mapa inicial con las minas y los alimentos
    matriz_inicial = leer_matriz_desde_archivo(nombre_archivo)
    matriz = matriz_inicial


    #Esto lo tengo que llamar en nada que reciba un mensaje del jugador
    matriz = limpiar_posicion(matriz, jugador['alias'])
    matriz, jugador = incluir_judarores(matriz, jugador)
    print(matriz)

    tiempo_ïnicial = time.time()
    #Este bucle es el que se ejecuta hasta que termine el juego. En caso de que no quede solo un jugador vivo
    while time.time() - tiempo_ïnicial < tiempo_juego:
        print(matriz)
        time.sleep(1)
        

    print("Ha terminado el tiempo de juego")


    '''producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer=lambda x: 
                            dumps(x).encode('utf-8'))
    '''
    '''for e in range(1000):
        data = {'number' : e}
        producer.send('engine-prueba', value=data)
        sleep(5) 
    '''