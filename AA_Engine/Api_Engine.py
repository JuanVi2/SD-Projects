import flask 
import json
import os
import sys
import requests
import time

from flask import Flask
from flask_cors import CORS, cross_origin

cert = 'cert.pem'
key = 'key.pem'

# a simple flask server to serve the API
app = flask.Flask(__name__)
api = requests.session()

cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route("/")
@cross_origin()
def my_form_post():
 return 'OK'

# simple funcion that retrun the map from file
def get_map():
    with open('mapa_juego.txt') as f:
        data = json.load(f)
    return data

@app.route('/map', methods=['GET'])

def get_map_api():
    return flask.jsonify(get_map())

if __name__ == "__main__":
    app.run(ssl_context= ('cert.pem', 'key.pem') , host='0.0.0.0', port=443)



