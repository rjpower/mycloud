#!/usr/bin/env python

'''Methods for connecting to and interacting with remote machines.'''

import atexit
import logging
import ssh
import subprocess
import sys
import threading

class SSH(object):
  connections = {}
  connections_lock = threading.Lock()

  def __init__(self, host):
    self.host = host
    self.lock = threading.Lock()
    self.client = ssh.SSHClient()
    self.client.set_missing_host_key_policy(ssh.AutoAddPolicy())
    self._connected = False

  def _connect(self):
    self.client.connect(self.host)
    self._connected = True

  def close(self):
    self.client.close()

  @staticmethod
  def connect(host):
    with SSH.connections_lock:
      if not host in SSH.connections:
        SSH.connections[host] = SSH(host)

    return SSH.connections[host]

  def invoke(self, command, *args):
    with self.lock:
      if not self._connected:
        self._connect()

    logging.info('Invoking %s %s', command, args)
    stdin, stdout, stderr = self.client.exec_command('PYTHONPATH=%s %s %s' % (':'.join(sys.path), command, ' '.join(args)))
    return stdin, stdout, stderr

  @staticmethod
  def shutdown():
    logging.info('Closing all SSH connections')
    for connection in SSH.connections.values():
      connection.close()
    SSH.connections = {}

class Local(object):
  @staticmethod
  def connect():
    return Local()

  def invoke(self, command, *args):
    p = subprocess.Popen([command] + list(args),
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=None)

    return (p.stdin, p.stdout, None)


atexit.register(SSH.shutdown)
