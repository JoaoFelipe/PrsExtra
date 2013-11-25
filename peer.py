#coding: utf-8

import socket
import json
import signal
import sys
import functools
import traceback
from datetime import datetime
from time import mktime
from tracker import TRACKER_PORT, CLOSE_PEERS

def signal_handler(peer, signal, frame): 
    peer.close()
    sys.exit(0)

class Peer(object):

    def __init__(self, tracker_ip, time, deltatime=0.0):
        self.next = None
        self.tracker_ip = tracker_ip
        self.datetime = time
        self.deltatime = deltatime

        print 'Conectando no Tracker'
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((self.tracker_ip, TRACKER_PORT))
        client.send(json.dumps({'op': 'add'}))
        self.this, self.next = json.loads(client.recv(1024))
        self.leader = self.this
        client.close()
        print 'Próximo peer:', self.next
        print 'Conexão com o Tracker encerrada'
        print 'Líder:', self.leader
        
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(('', self.this[1]))
        self.server.listen(5)
        self.running = True
        print 'Servidor do Peer aberto na porta', self.this[1]
        self.propagate_leader(self.this)

    def time(self):
        return mktime(self.datetime().timetuple()) + self.deltatime

    def propagate_leader(self, original):
        #if self.next != self.leader:
        print 'Propagando líder'
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(tuple(self.next))
        client.send(json.dumps({
            'op': 'leader', 
            'leader': self.leader, 
            'original': original,
            'time': str(self.time())
        }))
        client.close()


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
            elif msg['op'] == 'leader':
                original = msg['original']
                if original == self.this:
                    print 'Propagação voltou com tempos iguais. Usando lider do anel'
                    self.leader = msg['leader']
                    print 'Líder:', self.leader
                    continue
                received_time = float(msg['time'])
                current_time = self.time()
                print 'Líder:', self.leader
                print 'Próximo peer', self.next
                print 'Tempo recebido:',received_time
                print 'Tempo atual:', current_time
                if received_time > current_time:
                    self.leader = msg['leader']
                    self.deltatime += received_time - current_time
                    print 'Novo líder:', self.leader
                    self.propagate_leader(msg['original'])
                elif received_time == current_time:
                    self.propagate_leader(msg['original'])
                else:
                    self.propagate_leader(self.this)

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
    deltatime = 0.0
    if len(sys.argv) > 1:
        deltatime = float(sys.argv[-1])
    peer = Peer(socket.gethostname(), datetime.now, deltatime)
    signal.signal(signal.SIGINT, functools.partial(signal_handler, peer))
    try:
        peer.loop()
    except Exception:
        print traceback.format_exc()
        peer.close()
