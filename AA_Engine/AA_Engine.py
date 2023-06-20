from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random

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

# Esto lee el mapa inicial con las minas y los alimentos
nombre_archivo = 'mapa.txt'
tiempo_juego = 20
jugador = {
    'alias': 'Ponce',
    'password': '1234',
    'posicion': [0, 0],
    'nivel': 0,
    'ec': 0,
    'ef': 0,
}
posicion = [random.randint(0, 19), random.randint(0, 19)]
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
    

print("Ha terminado el tiempo de ejecución")


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

for e in range(1000):
    data = {'number' : e}
    producer.send('engine-prueba', value=data)
    sleep(5) 