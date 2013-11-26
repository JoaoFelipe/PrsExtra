# coding: UTF-8

import socket
import json
import sys


class Client(object):
    def __init__(self, address, name=None, logger=None):
        self.address = tuple(address)
        self.logger = logger
        self.name = name

    def __enter__(self):
        if self.logger:
            msg = u'Conectando em %s%s' % (pretty_address(self.address), u' (%s)' % self.name if self.name else '')
            self.logger.log(msg)
            self.logger.add_level()
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect(self.address)
        return self.client

    def __exit__(self, type, value, traceback):
        self.client.close()
        if self.logger:
            self.logger.log(u'Conexão encerrada')
            self.logger.remove_level()


class Logger(object):
    def __init__(self):
        self.ident = 0

    def log(self, msg):
        print(u'%s%s' % (self.ident * ' ', msg))

    def add_level(self):
        self.ident += 2

    def remove_level(self):
        self.ident -= 2
        if self.ident < 0:
            self.ident = 0


def signal_handler(peer, signal, frame): 
    peer.close()
    sys.exit(0)


def send_message(address, msg, name=None, logger=None):
    with Client(address, name=name, logger=logger) as client:
        client.send(json.dumps(msg))


def pretty_address(address):
    return u"%s:%i" % (address[0], address[1])
