# coding: UTF-8

import socket
import json
import signal
import sys
import functools
import traceback

DEFAULT_TRACKER_IP = ''
DEFAULT_TRACKER_PORT = 13617
FIRST_PORT = 14000
CLOSE_PEERS = True


def signal_handler(tracker, signal, frame): 
    tracker.close()
    sys.exit(0)


class Tracker(object):

    def __init__(self, ip=DEFAULT_TRACKER_IP, port=DEFAULT_TRACKER_PORT):
        self.peers = []
        self.current_port = FIRST_PORT

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((ip, port))
        self.server.listen(5)
        self.running = True
        print u'Tracker aberto na porta', port

    def add_peer(self, client, address):
        peer = (address[0], self.current_port)
        self.current_port += 1

        if self.peers:
            client.send(json.dumps((peer, self.peers[0])))
        else:
            client.send(json.dumps((peer, peer)))
        print u'Peer adicionado:', peer

        if len(self.peers) > 0:
            print u'Enviando endereço do peer', peer, u'para o peer', self.peers[-1]
            self.send_next_address(self.peers[-1], peer)

        self.peers.append(peer)
        print len(self.peers), u'peers'

    def remove_peer(self, peer):
        print u'Removendo peer:', peer
        if len(self.peers) > 1:
            index = self.peers.index(peer)
            previous_peer = self.peers[index - 1]
            next_peer = self.peers[(index + 1) % len(self.peers)]
            print u'Enviando endereço do peer', next_peer, u'para o peer', previous_peer
            self.send_next_address(previous_peer, next_peer)
            self.peers.pop(index)
        else:
            self.peers = []
        print len(self.peers), u'peers'

    def send_next_address(self, peer, next):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(peer)
        client.send(json.dumps({u'op': u'next', u'next': next}))
        client.close()

    def loop(self):
        while self.running:
            client, address = self.server.accept()
            print u'Peer conectado no endereço', address
            msg = json.loads(client.recv(1024))
            if msg[u'op'] == u'add':
                self.add_peer(client, address)
            elif msg[u'op'] == u'remove':
                self.remove_peer(tuple(msg[u'peer']))
            client.close()

    def close(self):
        if CLOSE_PEERS:
            print u'Fechando Peers'
            for peer in self.peers:
                print u'Fechando peer:', peer
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(peer)
                client.send(json.dumps({u'op': u'close'}))
                client.close()
        
        print u'Fechando Tracker'
        self.running = False
        self.server.close()


if __name__ == u'__main__':
    ip = DEFAULT_TRACKER_IP
    port = DEFAULT_TRACKER_PORT

    if u'-ip' in sys.argv:
        ip = sys.argv[2]
        if u'-pt' in sys.argv:
            port = sys.argv[4]
    elif u'-pt' in sys.argv:
        port = sys.argv[2]

    tracker = Tracker(ip, port)
    signal.signal(signal.SIGINT, functools.partial(signal_handler, tracker))
    try:
        tracker.loop()
    except Exception:
        print traceback.format_exc()
        tracker.close()
