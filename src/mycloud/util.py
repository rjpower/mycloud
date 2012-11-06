#!/usr/bin/env python

from SocketServer import TCPServer, ThreadingMixIn
from cloud.serialization import cloudpickle
import cPickle
import collections
import functools
import logging
import mycloud.thread
import os
import socket
import struct
import sys
import tempfile
import time
import traceback
import types
import xmlrpclib

def stacktraces():
  code = []
  for threadId, stack in sys._current_frames().items():
    code.append("\n# ThreadID: %s" % threadId)
    for filename, lineno, name, line in traceback.extract_stack(stack):
      code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
      if line:
        code.append("  %s" % (line.strip()))
  return code


class memoized(object):
  def __init__(self, func):
    self.func = func
    self.cache = {}
  
  def __call__(self, *args):
    assert isinstance(args, collections.Hashable)
    if args in self.cache:
      return self.cache[args]
    else:
      value = self.func(*args)
      self.cache[args] = value 
      return value

  def __repr__(self): return self.func.__doc__
  def __get__(self, obj, objtype): return functools.partial(self.__call__, obj)


class PeriodicLogger(object):
  def __init__(self, period):
    self.last = 0
    self.period = period

  def info(self, msg, *args, **kw):
    now = time.time()
    if now - self.last < self.period:
      return

    logging.info(msg, *args, **kw)
    self.last = now

def create_tempfile(dir, suffix):
  os.system("mkdir -p '%s'" % dir)
  return tempfile.NamedTemporaryFile(dir=dir, suffix=suffix)

class LoggingServer(TCPServer):
  def __init__(self):
    host = '0.0.0.0'
    port = logging.handlers.DEFAULT_TCP_LOGGING_PORT
    self.allow_reuse_address = True
    TCPServer.__init__(self, (host, port), None)
    self.timeout = 0.1
    self.controller = None

    # for each distinct host, keep track of the last message sent
    self.message_map = {}

  def attach(self, controller):
    self.controller = controller

  def detach(self):
    self.controller = None

  def finish_request(self, socket, client_address):
    header = socket.recv(4)
    if len(header) < 4:
      return

    rlen = struct.unpack('>L', header)[0]
    req = socket.recv(rlen)
    while len(req) < rlen:
      chunk = socket.recv(rlen - len(req))
      if chunk is None:
        return
      req += chunk

    record = logging.makeLogRecord(cPickle.loads(req))
    srchost = client_address[0]

    self.message_map[client_address] = record

    if record.exc_info and self.controller is not None:
      self.controller.report_exception(record.exc_info)
#      logging.info('Exception from %s.', srchost)
    else:
      logging.getLogger().handle(record)


def to_tuple(arglist):
  for i, args in enumerate(arglist):
    if not isinstance(args, types.TupleType):
      arglist[i] = (args,)

  return arglist

def find_open_port():
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(("", 0))
  s.listen(1)
  port = s.getsockname()[1]
  s.close()
  return port

class RemoteException(object):
  def __init__(self, type, value, tb):
    self.type = type
    self.value = value
    self.tb = traceback.format_exc(tb)
