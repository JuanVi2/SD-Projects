
import socket
import sqlite3
import threading
from sys import argv
import re
import bcrypt

from flask import Flask, request, jsonify
import logging
import ssl

app = Flask(__name__)

cert = 'cert.pem'
key = 'key.pem'

logging.basicConfig(filename='auditoria.txt', level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

registry = []


MAX_CONEXIONES = 15

def registrar_auditoria(ip, alias, accion, descripcion):
    mensaje = f'IP: {ip} | Alias: {alias} |Acción: {accion} | Descripción: {descripcion}'
    logging.info(mensaje)

def registrar_error_auditoria(alias, accion, descripcion):
    mensaje = f'Alias: {alias} |Acción: {accion} | Descripción: {descripcion}'
    logging.error(mensaje)

@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    alias = data['alias']
    passwd = data['passwd']
    nivel = data['nivel']
    ef = data['ef']
    ec = data['ec']
    print(f"REGISTRO alias: {alias} passwd: {passwd} nivel: {nivel} ef: {ef} ec: {ec} ")
    registrar_auditoria(request.remote_addr, alias, 'Alta', 'Registro de usuario')
    resultado = register(alias, passwd, nivel, ef, ec)
    if resultado:
        return jsonify({"status": "ok"})
    else:
        return jsonify({"status": "no"})
    
@app.route('/update', methods=['POST'])
def update():
    data = request.get_json()
    alias = data['alias']
    passwd = data['passwd']
    n_alias = data['n_alias']
    n_passwd = data['n_passwd']
    registrar_auditoria(request.remote_addr, alias, 'Modificación', 'Modificación de usuario')
    resultado = update(alias, passwd, n_alias, n_passwd)
    if resultado:
        return jsonify({"status": "ok"})
    else:
        return jsonify({"status": "no"})

@app.route('/delete', methods=['POST'])
def delete():
    data = request.get_json()
    alias = data['alias']
    passwd = data['passwd']
    registrar_auditoria(request.remote_addr, alias, 'Baja', 'Baja de usuario')
    resultado = delete(alias, passwd)
    if resultado:
        return jsonify({"status": "ok"})
    else:
        return jsonify({"status": "no"})

def register(alias: str, passwd: str, nivel: int, ef: int, ec: int) -> bool:
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

    cur.execute(f"select * from users where alias like '{alias}';")
    if cur.fetchone() is not None:
        print("ERROR: alias ya registrado")
        registrar_error_auditoria(alias, 'Alta', 'Alias ya registrado')
        return False
    
    hpasswd = bcrypt.hashpw(passwd.encode('utf-8'), bcrypt.gensalt())
    sql_command = "INSERT INTO users (alias, passwd, nivel, ef, ec) VALUES (?, ?, ?, ?, ?)"
    values = (alias, hpasswd, nivel, ef, ec)

    try:
        cur.execute(sql_command, values)
        con.commit()
        print(f"REGISTRADO CON EXITO ")
    except Exception as e:
        print("ERROR al registrar", e)
        registrar_error_auditoria(alias, 'Alta', 'Error al registrar')
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
    for row in cur.execute(f"select passwd from users where alias like '{alias}';"):
        bd_passwd = row[0]
    
    if bd_passwd is None:
        return False
    
    if bcrypt.checkpw(passwd.encode('utf-8'), bd_passwd):
        sol = True
    con.close()
    return sol

def delete(alias, passwd):
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    if login(alias, passwd) is False:
        print("ERROR: usuario no existe")
        registrar_error_auditoria(alias, 'Baja', 'Usuario no existe')
        return False
    try:
        sql_command = "DELETE FROM users WHERE alias = ?"
        values = (alias,)
        cur.execute(sql_command, values)
        con.commit()
        print(f"ELIMINADO CON EXITO ")
    except Exception as e:
        print("ERROR al eliminar", e)
        registrar_error_auditoria(alias, 'Baja', 'Error al eliminar')
        return False
    finally:
        con.close()
        return True
    




def update(alias, passwd, n_alias, n_passwd) -> bool:
    """
    :param alias: alias del usuario
    :param passwd: password del usuario
    :param n_alias: nuevo alias del usuario
    :param n_passwd: nueva password del usuario
    :return: True/False si se ha modificado con exito o no
    """
    

    if login(alias, passwd) is False:
        return False

    final = True
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    cur.execute(f"select * from users where alias like '{n_alias}';")
    if cur.fetchone() is not None:
        print("ERROR: nuevo alias ya registrado")
        registrar_error_auditoria(alias, 'Modificación', 'Nuevo alias ya registrado')
        return False
    
    try:
        if n_alias != '' and cur.fetchone() is None:
            sql_command = "UPDATE users SET alias = ? WHERE alias = ?"
            values = (n_alias, alias)
            cur.execute(sql_command, values)
            con.commit()
            print("actualizado alias")
        if n_passwd != '':
            hn_passwd = bcrypt.hashpw(n_passwd.encode('utf-8'), bcrypt.gensalt())
            sql_command = "UPDATE users SET passwd = ? WHERE alias = ?"
            values = (hn_passwd, n_alias)
            cur.execute(sql_command, values)
            con.commit()
            print("actualizado passwd")


        print(f"ACTUALIZADO CON EXITO ")
    except Exception as e:
        print("ERROR al actualizar, usuario no existe o el nuevo nombre ya esta escogido", e)
        registrar_error_auditoria(alias, 'Modificación', 'Error al actualizar')
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
        registrar_auditoria(addr, alias, 'Alta', 'Registro')
        resultado = register(alias, passwd, nivel, ef, ec)
    if modo == 'u':
        alias = credentials.split(":")[1]
        passwd = credentials.split(":")[2]
        n_alias = credentials.split(":")[3]
        n_passwd = credentials.split(":")[4]
        registrar_auditoria(addr, alias, 'Modificacion', 'Update')
        resultado = update(alias, passwd, n_alias, n_passwd)
    if modo == 'd':
        alias = credentials.split(":")[1]
        passwd = credentials.split(":")[2]
        registrar_auditoria(addr, alias, 'Baja', 'Delete')
        resultado = delete(alias, passwd)

    if resultado:

        conn.send(b'ok')
    else:
        print(f"NO SE HA PODIDO REGISTRAR/ACTUALIZAR: ")
        conn.send(b'no')
    conn.close()


def repartidor(server):


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
    if len(args) != 4:
        print("Numero incorrecto de argumentos")
        return False


    regex_1 = '^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]{1,5}$'
    regex_2 = '^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]{1,5}$'
    if not re.match(regex_1, args[1]) or not re.match(regex_2, args[2]):
        print("Argumentos incorrectos")
        return False
    return True

"""
Funcion que se encarga de manejar las peticiones de los clientes
"""
def handle_socket_requests():
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind((IP, PORT))
    try:
        repartidor(serversocket)
    except Exception as e:
        print("ERROR: ", e)
    finally:
        serversocket.close()

def handle_api_requests():
    app.run(ssl_context= ('cert.pem', 'key.pem'), host=IP_API, port=PORT_API)


if __name__ == '__main__':
    if not filtra(argv):
        print("ERROR: Argumentos incorrectos")
        print("Usage: AA_Registry.py <ip_socket:puerto> <ip_api:api> <fichero de base de datos>")
        print("Example: AA_Registry.py 127.0.0.1:5054 127.0.0.1:5055 players.db")
        exit()

    PORT = int(argv[1].split(":")[1])
    IP = argv[1].split(":")[0]
    IP_API = argv[2].split(":")[0]
    PORT_API = int(argv[2].split(":")[1])
    DATABASE = argv[3]
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS users (alias TEXT, passwd TEXT, nivel INT, ef INT, ec INT);")
    con.commit()
    con.close()
    
    socket_thread = threading.Thread(target=handle_socket_requests)
    api_thread = threading.Thread(target=handle_api_requests)

    socket_thread.start()
    api_thread.start()

    

   

