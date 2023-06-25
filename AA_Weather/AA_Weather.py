from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import loads
import re
from sys import argv
import signal
import random
from json import dumps

def comprobar(args: list) -> bool:

    if len(args) != 2:
        print("Numero incorrecto de argumentos")
        return False
    
    regex_1 = '^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]{1,5}$'
    if(not re.match(regex_1, argv[1])):
        print("Formato de ip incorrecto")
        return False
    return True

def signal_handler(sig, frame):
    """
    Maneja la flag de final para terminal el bucle infinito cuando se le manda SIGINT
    """
    global running
    print("TERMINANDO PROCESO DE TEMPERATURAS")
    running = False
    exit()

def run(producer: KafkaProducer) -> None:
    
    interrupted = False
    temperaturas = []
    with open("ciudades.txt", 'r') as archivo:
        for linea in archivo:
            ciudad, temperatura = linea.strip().split(',')
            temperaturas.append((ciudad, temperatura))
    
    ciudades_aleatorias = random.sample(temperaturas, 4)
    print(ciudades_aleatorias)

    data = {'ciudades' : ciudades_aleatorias}
    producer.send('Temperaturas', value=data)
   

if __name__ == '__main__':
    if not comprobar(argv):
        print("ERROR: Argumentos incorrectos")
        print("USO : AA_Weather.py <ip_servidor:puerto>")
        print("Example: AA_Weather.py 192.168.22.00:9092")
        exit()

    ip = argv[1].split(':')[0]
    port = int(argv[1].split(':')[1])

    signal.signal(signal.SIGINT, signal_handler)
    running = True
    encontrado = False
    value = None
    try:
        consumer = KafkaConsumer(
                                    'Acceso',
                                    bootstrap_servers=f'{ip}:{port}',
                                    auto_offset_reset='latest',
                                    enable_auto_commit=True,
                                    group_id='Weather-group',
                                    value_deserializer=lambda x: loads(x.decode('utf-8')))
        
        while not(encontrado) and running:
            for message in consumer:
                if message.value['Entrada'] == False:
                    value = message.value
                    print(f"Mensaje recibido: valor: {value}")
                    
                else:
                    value = message.value
                    entontrado = True
                    print(f"Mensaje recibido: valor: {value}")
                    break
                    

            producer = KafkaProducer(bootstrap_servers=f'{ip}:{port}',
                                        value_serializer=lambda x: 
                                        dumps(x).encode('utf-8'))
            if(value['Entrada'] == True):
                run(producer)
            
    except Exception as e:
        print("ERROR: ", e)
    finally:
        if 'producer' in locals():
            producer.close()
    exit(0)

