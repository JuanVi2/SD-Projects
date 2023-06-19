import socket
import sqlite3
import threading
from sys import argv
import re


IP = socket.gethostbyname(socket.gethostname())
MAX_CONEXIONES = 2


def registrar(alias: str, passwd: str, nivel: int, ef: int, ec: int) -> bool:
    """
    :param alias: alias del usuario
    :param passwd: password que va a usar el usuario
    :param nivel: nivel del usuario
    :param ef: efecto frio del usuario
    :param ec: efecto calor del usuario
    :return: True/False si se ha registrado con exito o no
    """
    final = True
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()

    sql_comand = f"insert into users (alias, passwd, nivel, ef, ec)" \
                 f" values ('{alias}','{passwd}','{nivel}','{ef}','{ec}');"
    try:
        cur.execute(sql_comand)
        con.commit()
        print(f"REGISTRADO CON EXITO ")
    except Exception as e:
        print("ERROR al registrar", e)
        final = False
    finally:
        con.close()
        return final
    


def login(alias, passwd):
    """
    :param alias: alias del usuario
    :param passwd: password del usuario
    :return: True/False si se ha logeado con exito o no
    """
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    sol = False
    for _ in cur.execute(f"select * from users "
                         f"where "
                         f"alias like '{alias}' and "
                         f"passwd like '{passwd}' ;"):
        sol = True
    con.close()
    return sol



def modificar(alias, passwd, n_alias, n_passwd) -> bool:
    """
    :param alias: alias del usuario
    :param passwd: password del usuario
    :param n_alias: nuevo alias del usuario
    :param n_passwd: nueva password del usuario
    :return: True/False si se ha modificado con exito o no
    """

    if not login(alias, passwd):
        return False

    final = True
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()

    try:
        if n_alias != '':
            cur.execute(f"update users set alias = '{n_alias}' where alias like '{alias}';")
            alias = n_alias
            print("actualizado alias")
        if n_passwd != '':
            cur.execute(f"update users set passwd = '{n_passwd}' where alias like '{alias}';")
            print("actualizado passwd")

        con.commit()
        print(f"ACTUALIZADO CON EXITO ")
    except Exception as e:
        print("ERROR al registrar", e)
        final = False
    finally:
        con.close()
        return final
    
    
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

    """
    Modos:
    r: registrar
    u: actualizar
    """

    if modo == 'r':
        alias = credentials.split(":")[1]
        passwd = credentials.split(":")[2]
        nivel = credentials.split(":")[3]
        ef = credentials.split(":")[4]
        ec = credentials.split(":")[5]
        print(f"REGISTRO alias: {alias} passwd: {passwd} nivel: {nivel} ef: {ef} ec: {ec} ")
        resultado = registrar(alias, passwd, nivel, ef, ec)
    if modo == 'u':
        alias = credentials.split(":")[1]
        passwd = credentials.split(":")[2]
        n_alias = credentials.split(":")[3]
        n_passwd = credentials.split(":")[4]
        resultado = modificar(alias, passwd, n_alias, n_passwd)

    if resultado:

        conn.send(b'ok')
    else:
        print(f"NO SE HA PODIDO REGISTRAR/ACTUALIZAR: ")
        conn.send(b'no')
    conn.close()


def repartidor(server):
    """
    :param server: servidor
    """

    server.listen()
    print(f"ESCUCHANDO EN {IP}:{PORT}")
    num_conexiones = threading.active_count() - 1
    print(f"N CONEXIONES ACTUAL: {num_conexiones}")
    
    while True:
        conn, addr = server.accept()
        num_conexiones = threading.active_count()
        if num_conexiones >= MAX_CONEXIONES:
            print("LIMITE DE CONEXIONES EXCEDIDO")
            conn.send(b"EL SERVIDOR HA EXCEDIDO EL LIMITE DE CONEXIONES")
            conn.close()
            num_conexiones = threading.active_count() - 1
        else:
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {num_conexiones}")
            print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES - num_conexiones)




def filtra(args: list) -> bool:
    """
    Indica si el formato de los argumentos es el correcto
    :param args: Argumentos del programa
    """
    if len(args) != 3:
        print("Numero incorrecto de argumentos")
        return False

    """
    esto hace"""
    regex_1 = '^[0-9]{1,5}$'
    if not re.match(regex_1, args[1]):
        print("Puerto incorrecto")
        return False
    return True


if __name__ == '__main__':
    if not filtra(argv):
        print("ERROR: Argumentos incorrectos")
        print("Usage: AA_Registry.py <puerto> <fichero de base de datos>")
        print("Example: AA_Registry.py 5054 ")
        exit()

    PORT = int(argv[1])
    DATABASE = argv[2]

    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind((IP, PORT))
    try:
        repartidor(serversocket)
    except Exception as e:
        print("ERROR: ", e)
    finally:
        serversocket.close()
