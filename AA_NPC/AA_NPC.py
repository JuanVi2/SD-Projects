from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import loads
import re
from sys import argv
import signal
import random
import uuid
from json import dumps
playerId = str(uuid.uuid4())

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
        'id': playerId,
        'posicion' : posicion,
        'nivel' : nivel,
    }
    global NPC
    NPC.append(npc)

def run(producer: KafkaProducer) -> None:
    
    interrupted = False
    
    crearNPC()
    
    data = {'npcs' : NPC}
    producer.send('Npc', value=data)
    print(NPC)


if __name__ == '__main__':
    if not comprobar(argv):
        print("ERROR: Argumentos incorrectos")
        print("USO : AA_NPC.py <ip_servidor:puerto>")
        print("Example: AA_NPC.py 127.0.0.1:9092")
        exit()

    ip = argv[1].split(':')[0]
    port = int(argv[1].split(':')[1])
    
    NPC = []
    signal.signal(signal.SIGINT, signal_handler)
    fin = False
    value = None
    try:
        
        producer = KafkaProducer(bootstrap_servers=f'{ip}:{port}',
            value_serializer=lambda x: 
            dumps(x).encode('utf-8'))
        run(producer)
        
        playerInfoConsumer = KafkaConsumer(
                            playerId + 'in',
                            bootstrap_servers=f'{ip}:{port}',
                            enable_auto_commit=True,
                            group_id='test',
                            value_deserializer=lambda x: loads(x.decode('utf-8')))
        
        for message in playerInfoConsumer:
                print("Esperando en el topic propio")

                

                print(message.value['id'])
                print(f"Mapa recibido: {message.value['mapa']}")
                if(message.value['nivel'] == -1):
                    print("NPC muerto")
                    exit(0)
                
                sleep(10)
                
                NPC = [{'id': playerId, 'posicion': [random.randint(0, 19), random.randint(0, 19)], 'nivel': NPC[0]['nivel']}]
                data = {'npcs' : NPC}
                print("Estos son los datos que envio para moverme: ")
                print(data)
                producer.send('Npc', value=data)
                print("Enviada nueva posición") 
        
            
    except Exception as e:
        print("ERROR: ", e)
    finally:
        if 'producer' in locals():
            producer.close()
    exit(0)

