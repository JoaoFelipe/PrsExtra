# coding: UTF-8

import socket
import json
import signal
import sys
import functools
import traceback
from optparse import OptionParser
from base import Client, Logger, pretty_address

SOCKET_LISTENER_IP = ''
DEFAULT_TRACKER_PORT = 13617
FIRST_PORT = 14000
CLOSE_PEERS = True


def signal_handler(tracker, signal, frame): 
    tracker.close()
    sys.exit(0)


class Tracker(object):

    def __init__(self, port=DEFAULT_TRACKER_PORT):
        self.peers = []
        self.current_port = FIRST_PORT
        self.logger = Logger()

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((SOCKET_LISTENER_IP, port))
        self.server.listen(5)
        self.running = True
        self.logger.log(u'Tracker aberto na porta {0}'.format(port))
        self.logger.add_level()

    def add_peer(self, client, address):
        peer = (address[0], self.current_port)
        self.current_port += 1

        if self.peers:
            client.send(json.dumps((peer, self.peers[0])))
        else:
            client.send(json.dumps((peer, peer)))
        self.logger.log(u'Peer adicionado: {0}'.format(pretty_address(peer)))

        if len(self.peers) > 0:
            self.logger.log(u'Enviando endereço do peer {0} para o peer {1}'.format(pretty_address(peer), pretty_address(self.peers[-1])))
            self.send_next_address(self.peers[-1], peer)

        self.peers.append(peer)
        self.logger.log(u'{0} peer(s)'.format(len(self.peers)))

    def remove_peer(self, peer):
        self.logger.log(u'Removendo peer: {0}'.format(peer))
        if len(self.peers) > 1:
            index = self.peers.index(peer)
            previous_peer = self.peers[index - 1]
            next_peer = self.peers[(index + 1) % len(self.peers)]
            self.logger.log(u'Enviando endereço do peer {0} para o peer {1}'.format(pretty_address(next_peer), pretty_address(previous_peer)))
            self.send_next_address(previous_peer, next_peer)
            self.peers.pop(index)
        else:
            self.peers = []
        self.logger.log(u'{0} peer(s)'.format(len(self.peers)))

    def send_next_address(self, peer, next):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(peer)
        client.send(json.dumps({u'op': u'next', u'next': next}))
        client.close()

    def loop(self):
        while self.running:
            client, address = self.server.accept()
            self.logger.log(u'Peer conectado no endereço {0}'.format(pretty_address(address)))
            msg = json.loads(client.recv(1024))
            if msg[u'op'] == u'add':
                self.add_peer(client, address)
            elif msg[u'op'] == u'remove':
                self.remove_peer(tuple(msg[u'peer']))
            client.close()

    def close(self):
        self.logger.ident = 0
        if CLOSE_PEERS:
            self.logger.log(u'Fechando Peers')
            for peer in self.peers:
                self.logger.log(u'Fechando peer: {0}'.format(pretty_address(peer)))
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(peer)
                client.send(json.dumps({u'op': u'close'}))
                client.close()
        
        self.logger.log(u'Fechando Tracker')
        self.running = False
        self.server.close()

def parse_options():
    usage = u"%prog [OPÇÕES]"
    parser = OptionParser(usage)
    parser.add_option(
        '-p',
        '--port',
        action = 'store',
        type = 'int',
        dest = 'port',
        default = DEFAULT_TRACKER_PORT,
    )
    return parser.parse_args()

if __name__ == u'__main__':
    options, args = parse_options()

    tracker = Tracker(options.port)
    signal.signal(signal.SIGINT, functools.partial(signal_handler, tracker))
    try:
        tracker.loop()
    except Exception:
        print traceback.format_exc()
        tracker.close()
