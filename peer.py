# coding: UTF-8

import socket
import json
import signal
import sys
import functools
import traceback
import time
from base import (
    Client,
    Logger,
    pretty_address,
    send_message,
    signal_handler,
)
from tracker import (
    CLOSE_PEERS,
    DEFAULT_TRACKER_IP,
    DEFAULT_TRACKER_PORT,
)


# Essa classe trata da conexão entre peer e tracker e a formação do anél
class TrackerPeer(object):

    def __init__(self, tracker_ip=DEFAULT_TRACKER_IP, tracker_port=DEFAULT_TRACKER_PORT):
        self.this = None
        self.next = None
        self.tracker_ip = tracker_ip
        self.tracker_port = tracker_port
        self.logger = Logger()

        with Client((self.tracker_ip, self.tracker_port), name=u'Tracker', logger=self.logger) as client:
            client.send(json.dumps({u'op': u'add'}))
            self.this, self.next = json.loads(client.recv(1024))
                
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.tracker_ip, self.this[1]))
        self.server.listen(5)
        
        self.listeners = {
            u'close': self.receive_close,
            u'next': self.receive_next,
        }

        self.running = True
        self.logger.log(u'Servidor do Peer aberto em {0}'.format(pretty_address((socket.gethostname(), self.this[1]))))
        self.logger.add_level()

    def send_message_to_tracker(self, message):
        send_message((self.tracker_ip, self.tracker_port), message, name=u'Tracker', logger=self.logger)

    def send_message_to_next(self, message):
        send_message(self.next, message, name=u'Próximo', logger=self.logger)

    def info_print(self):
        self.logger.log(u'Endereço: {0}'.format(pretty_address(self.this)))
        self.logger.log(u'Próximo: {0}'.format(pretty_address(self.next)))

    def receive_close(self, msg):
        if CLOSE_PEERS:
            self.close(notify_tracker=False)

    def receive_next(self, msg):
        self.next = msg[u'next']
        self.logger.log(u'Próximo peer alterado')
        self.info_print()

    def loop(self):
        while self.running:
            client, address = self.server.accept()
            self.logger.log(u'Peer conectado no endereço %s' % pretty_address(address))
            self.logger.add_level()
            msg = json.loads(client.recv(1024))
            self.logger.log(u'Operação "{0}"'.format(msg[u'op']))
            self.listeners[msg[u'op']](msg)
            client.close()
            self.logger.log(u'Peer desconectado')
            self.logger.remove_level()

    def close(self, notify_tracker=True):
        self.logger.ident = 0
        self.logger.log(u'Fechando Peer')
        if notify_tracker:
            self.send_message_to_tracker({
                u'op': u'remove',
                u'peer': self.this
            })
        self.close_extra(notify_tracker)
        self.running = False
        self.server.close()


# Essa classe trata da propagação do tempo/lider no anel
class Peer(TrackerPeer):

    def __init__(self, tracker_ip=DEFAULT_TRACKER_IP, tracker_port=DEFAULT_TRACKER_PORT, delta_time=0.0):
        TrackerPeer.__init__(self, tracker_ip, tracker_port)
        self.peer_time = time.time()
        self.delta_time = delta_time
            
        self.leader = self.this

        self.listeners[u'leader'] = self.receive_leader
        self.listeners[u'disconnect_leader_prop'] = self.receive_disconnect_leader_prop
        self.listeners[u'disconnect_leader'] = self.receive_disconnect_leader
        
        self.info_print()
        if self.next != self.this:
            self.propagate_leader(self.this)

    def info_print(self):
        TrackerPeer.info_print(self)
        self.logger.log(u'Líder: {0}'.format(pretty_address(self.leader)))
        self.logger.log(u'Hora: {0}'.format(self.get_time()))

    def get_time(self):
        return self.peer_time + self.delta_time
    
    def propagate_leader(self, original):
        self.logger.log(u'Propagando líder')
        self.send_message_to_next({
            u'op': u'leader',
            u'leader': self.leader,
            u'original': original,
            u'time': str(self.get_time())
        })

    def propagate_leader_disconnect(self, original):
        self.logger.log(u'Propagando saída de líder')
        self.send_message_to_next({
            u'op': u'disconnect_leader_prop',
            u'new_leader': self.leader,
            u'original': original
        })

    def disconnect_leader(self):
        self.logger.log(u'Enviando saída de líder')
        self.send_message_to_next({u'op': u'disconnect_leader'})

    def receive_leader(self, msg):
        current_time = self.get_time()
        received_time = float(msg[u'time'])
        self.logger.add_level()
        self.logger.log(u'Hora recebida: {0}'.format(received_time))
        self.logger.log(u'Líder recebido: {0}'.format(pretty_address(msg[u'leader'])))
        self.logger.log(u'Original recebido: {0}'.format(pretty_address(msg[u'original'])))
        self.logger.remove_level()
        
        original = msg[u'original']
        if original == self.this:
            self.logger.log(u'Propagação voltou com tempos iguais. Usando líder do anel')
            self.leader = msg[u'leader']
            self.info_print()
            return

        if received_time > current_time:
            self.logger.log(u'Tempo recebido maior do que tempo atual. Atualizando líder')
            self.leader = msg[u'leader']
            self.delta_time += received_time - current_time
            self.propagate_leader(msg[u'original'])
        elif received_time == current_time:
            self.propagate_leader(msg[u'original'])
        else:
            self.logger.log(u'Este é o novo líder do anél. Propagando informação')
            self.propagate_leader(self.this)
        self.info_print()

    def receive_disconnect_leader_prop(self, msg):
        self.logger.add_level()
        self.logger.log(u'Novo Líder recebido: {0}'.format(pretty_address(msg[u'new_leader'])))
        self.logger.log(u'Original recebido: {0}'.format(pretty_address(msg[u'original'])))
        self.logger.remove_level()

        original = msg[u'original']
        if original == self.this:
            self.logger.log(u'Líder antigo removido de todo anél. Verificando líder novo')
            return self.propagate_leader(original)
        self.logger.log(u'Líder antigo desconectou do anél. Utilizando sucessor')
        self.leader = msg[u'new_leader']
        self.propagate_leader_disconnect(msg[u'original'])
        self.info_print()

    def receive_disconnect_leader(self, msg):
        self.logger.log(u'Líder antigo desconectou do anél. Assumirei como novo líder')
        old_leader = self.leader
        self.leader = self.this
        if self.next != old_leader:
            self.propagate_leader_disconnect(self.this)
        self.info_print()

    def close_extra(self, notify_tracker=True):
        if self.leader == self.this:
            self.disconnect_leader()


if __name__ == u'__main__':
    tracker_ip = DEFAULT_TRACKER_IP
    tracker_port = DEFAULT_TRACKER_PORT
    delta_time = 0.0

    if u'-ip' in sys.argv:
        tracker_ip = sys.argv[2]
        if u'-dt' in sys.argv:
            delta_time = sys.argv[4]
    elif u'-pt' in sys.argv:
        tracker_port = sys.argv[2]
        if u'-dt' in sys.argv:
            delta_time = sys.argv[4]
    elif u'-dt' in sys.argv:
            delta_time = sys.argv[2]

    peer = Peer(tracker_ip, tracker_port, delta_time)
    signal.signal(signal.SIGINT, functools.partial(signal_handler, peer))
    try:
        peer.loop()
    except Exception:
        print traceback.format_exc()
        peer.close()
