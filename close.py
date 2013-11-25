import json
import socket

print socket.gethostname()
peer = ('127.0.0.1', 14000)
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(peer)
client.send(json.dumps({'op': 'close'}))
client.close()