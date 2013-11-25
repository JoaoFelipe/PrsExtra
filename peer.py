#coding: utf-8

import socket
import json
import signal
import sys
import functools
import traceback
from tracker import TRACKER_PORT, CLOSE_PEERS

def signal_handler(peer, signal, frame): 
    peer.close()
    sys.exit(0)

class Peer(object):

    def __init__(self, tracker_ip):
        self.next = None
        self.tracker_ip = tracker_ip

        print 'Conectando no Tracker'
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((self.tracker_ip, TRACKER_PORT))
        client.send(json.dumps({'op': 'add'}))
        self.this, self.next = json.loads(client.recv(1024))
        client.close()
        print 'Próximo peer:', self.next
        print 'Conexão com o Tracker encerrada'

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(('0.0.0.0', self.this[1]))
        self.server.listen(5)
        self.running = True
        print 'Servidor do Peer aberto na porta', self.this[1]

    def loop(self):
        while self.running:
            client, address = self.server.accept()
            print 'Peer conectado no endereço', address
            msg = json.loads(client.recv(1024))
            if CLOSE_PEERS and msg['op'] == 'close':
                self.close(notify_tracker=False)
            elif msg['op'] == 'next':
                self.next = msg['next']
                print 'Próximo peer:', self.next
            client.send('Recebido')
            client.close()

    def close(self, notify_tracker=True):
        print 'Fechando Peer'
        if notify_tracker:
            # TODO: se for líder, passar líder para o próximo
            # Sair do Tracker
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((self.tracker_ip, TRACKER_PORT))
            client.send(json.dumps({'op': 'remove', 'peer': self.this}))
            client.close()

        self.running = False
        self.server.close()

if __name__ == '__main__':
    peer = Peer(socket.gethostname())
    signal.signal(signal.SIGINT, functools.partial(signal_handler, peer))
    try:
        peer.loop()
    except Exception:
        print traceback.format_exc()
        peer.close()
