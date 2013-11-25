#coding: utf-8

import socket
import json
import signal
import sys
import functools
import traceback

TRACKER_PORT = 13617
FIRST_PORT = 14000
CLOSE_PEERS = True

def signal_handler(tracker, signal, frame): 
    tracker.close()
    sys.exit(0)

class Tracker(object):

    def __init__(self):
        self.peers = []
        self.current_port = FIRST_PORT

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(('0.0.0.0', TRACKER_PORT))
        self.server.listen(5)
        self.running = True
        print 'Tracker aberto na porta', TRACKER_PORT

    def add_peer(self, client, address):
        peer = (address[0], self.current_port)
        self.current_port += 1

        if self.peers:
            client.send(json.dumps((peer, self.peers[0])))
        else:
            client.send(json.dumps((peer, peer)))
        print 'Peer adicionado:', peer

        if len(self.peers) > 0:
            print 'Enviando endereço do peer', peer, 'para o peer', self.peers[-1]
            self.send_next_address(self.peers[-1], peer)


        self.peers.append(peer)
        print len(self.peers), 'peers'

    def remove_peer(self, peer):
        print 'Removendo peer:', peer
        if len(self.peers) > 1:
            index = self.peers.index(peer)
            previous_peer = self.peers[index - 1]
            next_peer = self.peers[(index + 1) % len(self.peers)]
            print 'Enviando endereço do peer', next_peer, 'para o peer', previous_peer
            self.send_next_address(previous_peer, next_peer)
            self.peers.pop(index)
        else:
            self.peers = []
        print len(self.peers), 'peers'

    def send_next_address(self, peer, next):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(peer)
        client.send(json.dumps({'op': 'next', 'next': next}))
        client.close()


    def loop(self):
        while self.running:
            client, address = self.server.accept()
            print 'Peer conectado no endereço', address
            msg = json.loads(client.recv(1024))
            if msg['op'] == 'add':
                self.add_peer(client, address)
            elif msg['op'] == 'remove':
                self.remove_peer(tuple(msg['peer']))
            client.close()

    def close(self):
        if CLOSE_PEERS:
            print 'Fechando Peers'
            for peer in self.peers:
                print 'Fechando peer:', peer
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(peer)
                client.send(json.dumps({'op': 'close'}))
                client.close()
        
        print 'Fechando Tracker'
        self.running = False
        self.server.close()

if __name__ == '__main__':
    tracker = Tracker()
    signal.signal(signal.SIGINT, functools.partial(signal_handler, tracker))
    try:
        tracker.loop()
    except Exception:
        print traceback.format_exc()
        tracker.close()
