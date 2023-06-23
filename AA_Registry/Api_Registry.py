from flask import Flask, request, jsonify

app = Flask(__name__)

# Ruta para el registro de usuarios
@app.route('/registro', methods=['POST'])
def registro():
    # Obtener los datos del usuario desde la solicitud
    datos_usuario = request.get_json()
    
    # Aquí puedes realizar validaciones de los datos ingresados
    
    # Guardar el usuario en la base de datos o realizar cualquier otra operación
    
    # Devolver una respuesta
    return jsonify({'mensaje': 'Usuario registrado correctamente'})

if __name__ == '__main__':
    app.run(debug=True)

