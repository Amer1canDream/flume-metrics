from flask import Flask, jsonify
import os
import json

httpServer = Flask(__name__)

@httpServer.route('/zabbix/flume', methods=['GET'])
def getJson():
    file = open('data.json')
    data = file.read()
    file.close()
    return data

if __name__ == '__main__':
    httpServer.run(host = '0.0.0.0',port=5005)
