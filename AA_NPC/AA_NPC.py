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
    print("TERMINANDO PROCESO DE NPC")
    running = False
    exit()


def crearNPC() -> None:
    nivel = random.randint(1, 10)
    posicion = [random.randint(0, 19), random.randint(0, 19)]
    npc = {
        'posicion' : posicion,
        'nivel' : nivel,
    }
    NPC.append(npc)

def run(producer: KafkaProducer) -> None:
    
    interrupted = False
    for i in range(cantidad):
        crearNPC()
    
    data = {'npcs' : NPC}
    producer.send('Npc', value=data)


if __name__ == '__main__':
    if not comprobar(argv):
        print("ERROR: Argumentos incorrectos")
        print("USO : AA_NPC.py <ip_servidor:puerto>")
        print("Example: AA_NPC.py 127.0.0.1:9092")
        exit()

    ip = argv[1].split(':')[0]
    port = int(argv[1].split(':')[1])
    cantidad = int(argv[2])
    NPC = []
    signal.signal(signal.SIGINT, signal_handler)
    running = True
    encontrado = False
    value = None
    try:
        consumer = KafkaConsumer(
                                    'Acceso',
                                    bootstrap_servers=f'{ip}:{port}',
                                    auto_offset_reset='earliest',
                                    enable_auto_commit=True,
                                    group_id='Npc-group',
                                    value_deserializer=lambda x: loads(x.decode('utf-8')))
        
        while not(encontrado) and running:
            for message in consumer:
                if message.value == False:
                    value = message.value
                    print(f"Mensaje recibido: valor: {value}")
                    
                else:
                    encontrado = True
                    value = message.value
                    print(f"Mensaje recibido: valor: {value}")
                    

            print(f"Mensaje recibido: valor: {value}")

            producer = KafkaProducer(bootstrap_servers=f'{ip}:{port}',
                                        value_serializer=lambda x: 
                                        dumps(x).encode('utf-8'))
            if(value == True):
                run(producer)
            
    except Exception as e:
        print("ERROR: ", e)
    finally:
        if 'producer' in locals():
            producer.close()
    exit(0)

