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
from base import Client, send_message, signal_handler, pretty_address
from base import Logger

# Essa classe trata da conexão entre peer e tracker e a formação do anél
class TrackerPeer(object):

    def __init__(self, tracker_ip):
        self.this = None
        self.next = None
        self.tracker_ip = tracker_ip
        self.logger = Logger()

        with Client((self.tracker_ip, TRACKER_PORT), name='Tracker', logger=self.logger) as client:
            client.send(json.dumps({'op': 'add'}))
            self.this, self.next = json.loads(client.recv(1024))
                
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(('', self.this[1]))
        self.server.listen(5)
        
        self.listeners = {
            'close': self.receive_close,
            'next': self.receive_next,
        }

        self.running = True
        self.logger.log('Servidor do Peer aberto em {0}'.format(pretty_address((socket.gethostname(), self.this[1]))))
        self.logger.add_level()

    def send_message_to_tracker(self, message):
        send_message((self.tracker_ip, TRACKER_PORT), message, name='Tracker', logger=self.logger)

    def send_message_to_next(self, message):
        send_message(self.next, message, name='Próximo', logger=self.logger)

    def info_print(self):
        self.logger.log('Endereço: {0}'.format(pretty_address(self.this)))
        self.logger.log('Próximo: {0}'.format(pretty_address(self.next)))

    def receive_close(self, msg):
        if CLOSE_PEERS:
            self.close(notify_tracker=False)

    def receive_next(self, msg):
        self.next = msg['next']
        self.logger.log('Próximo peer alterado')
        self.info_print()

    def loop(self):
        while self.running:
            client, address = self.server.accept()
            self.logger.log('Peer conectado no endereço %s' % pretty_address(address))
            self.logger.add_level()
            msg = json.loads(client.recv(1024))
            self.logger.log('Operação "{0}"'.format(msg['op']))
            self.listeners[msg['op']](msg)
            client.close()
            self.logger.log('Peer desconectado')
            self.logger.remove_level()

    def close(self, notify_tracker=True):
        self.logger.ident = 0
        self.logger.log('Fechando Peer')
        if notify_tracker:
            self.send_message_to_tracker({
                'op': 'remove', 
                'peer': self.this
            })
        self.close_extra(notify_tracker)
        self.running = False
        self.server.close()


        
# Essa classe trata da propagação do tempo/lider no anel
class Peer(TrackerPeer):

    def __init__(self, tracker_ip, time, deltatime=0.0):
        TrackerPeer.__init__(self, tracker_ip)
        self.datetime = time
        self.deltatime = deltatime
            
        self.leader = self.this

        self.listeners['leader'] = self.receive_leader
        self.listeners['disconnect_leader_prop'] = self.receive_disconnect_leader_prop
        self.listeners['disconnect_leader'] = self.receive_disconnect_leader
        
        self.info_print()
        if self.next != self.this:
            self.propagate_leader(self.this)

    def info_print(self):
        TrackerPeer.info_print(self)
        self.logger.log('Líder: {0}'.format(pretty_address(self.leader)))
        self.logger.log('Hora: {0}'.format(self.time()))


    def time(self):
        return mktime(self.datetime().timetuple()) + self.deltatime
    
    def propagate_leader(self, original):
        self.logger.log('Propagando líder')
        self.send_message_to_next({
            'op': 'leader', 
            'leader': self.leader, 
            'original': original,
            'time': str(self.time())
        })

    def propagate_leader_disconnect(self, original):
        self.logger.log('Propagando saída de líder')
        self.send_message_to_next({
            'op': 'disconnect_leader_prop', 
            'new_leader': self.leader, 
            'original': original
        })

    def disconnect_leader(self):
        self.logger.log('Enviando saída de líder')
        self.send_message_to_next({'op': 'disconnect_leader'})

    def receive_leader(self, msg):
        current_time = self.time()
        received_time = float(msg['time'])
        self.logger.add_level()
        self.logger.log('Hora recebida: {0}'.format(received_time))
        self.logger.log('Líder recebido: {0}'.format(pretty_address(msg['leader'])))
        self.logger.log('Original recebido: {0}'.format(pretty_address(msg['original'])))
        self.logger.remove_level()
        
        original = msg['original']
        if original == self.this:
            self.logger.log('Propagação voltou com tempos iguais. Usando líder do anel')
            self.leader = msg['leader']
            self.info_print()
            return

        if received_time > current_time:
            self.logger.log('Tempo recebido maior do que tempo atual. Atualizando líder')
            self.leader = msg['leader']
            self.deltatime += received_time - current_time
            self.propagate_leader(msg['original'])
        elif received_time == current_time:
            self.propagate_leader(msg['original'])
        else:
            self.logger.log('Este é o novo líder do anél. Propagando informação')
            self.propagate_leader(self.this)
        self.info_print()

    def receive_disconnect_leader_prop(self, msg):
        self.logger.add_level()
        self.logger.log('Novo Líder recebido: {0}'.format(pretty_address(msg['new_leader'])))
        self.logger.log('Original recebido: {0}'.format(pretty_address(msg['original'])))
        self.logger.remove_level()

        original = msg['original']
        if original == self.this:
            self.logger.log('Líder antigo removido de todo anél. Verificando líder novo')
            return self.propagate_leader(original)
        self.logger.log('Líder antigo desconectou do anél. Utilizando sucessor')
        self.leader = msg['new_leader']
        self.propagate_leader_disconnect(msg['original'])
        self.info_print()

    def receive_disconnect_leader(self, msg):
        self.logger.log('Líder antigo desconectou do anél. Assumirei como novo líder')
        old_leader = self.leader
        self.leader = self.this
        if self.next != old_leader:
            self.propagate_leader_disconnect(self.this)
        self.info_print()

    def close_extra(self, notify_tracker=True):
        if self.leader == self.this:
            self.disconnect_leader()

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
