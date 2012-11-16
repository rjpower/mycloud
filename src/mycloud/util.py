#!/usr/bin/env python

import SocketServer
import cPickle
import collections
import functools
import logging
import logging.handlers
import os
import select
import socket
import struct
import sys
import tempfile as tf
import time
import traceback
import types

TEMP_DIR = tf.gettempdir()

class ClusterException(Exception):
  pass

class WorkerException(object):
  def __init__(self, tb):
    self.tb = tb
    

def stacktraces():
  '''Return a formatted list of all the current thread stacks.'''
  code = []
  for threadId, stack in sys._current_frames().items():
    code.append("\n# ThreadID: %s" % threadId)
    for filename, lineno, name, line in traceback.extract_stack(stack):
      code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
      if line:
        code.append("  %s" % (line.strip()))
  return code

def tempfile(suffix, directory=TEMP_DIR):
  os.system("mkdir -p '%s'" % TEMP_DIR)
  return tf.NamedTemporaryFile(dir=directory, suffix=suffix)

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

def set_non_blocking(f):
  import fcntl
  flags = fcntl.fcntl(f, fcntl.F_GETFL, 0)
  fcntl.fcntl(f, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    
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


  
class LogRecordStreamHandler(SocketServer.StreamRequestHandler):
  def handle(self):
    while True:
      chunk = self.connection.recv(4)
      if len(chunk) < 4:
        break
      slen = struct.unpack('>L', chunk)[0]
      chunk = self.connection.recv(slen)
      while len(chunk) < slen:
        chunk = chunk + self.connection.recv(slen - len(chunk))
      obj = cPickle.loads(chunk)
      record = logging.makeLogRecord(obj)
      logging.getLogger().handle(record)
      controller = self.server.controller
      if record.exc_info and controller is not None:
        controller.report_exception(record.exc_info)
      

class LoggingServer(SocketServer.ThreadingTCPServer):
  allow_reuse_address = 1

  def __init__(self, host='0.0.0.0',
         port=logging.handlers.DEFAULT_TCP_LOGGING_PORT,
         handler=LogRecordStreamHandler):
    SocketServer.ThreadingTCPServer.__init__(self, (host, port), handler)
    self.abort = 0
    self.timeout = 1
    self.logname = None
    self.controller = None
    
  def attach(self, controller):
    self.controller = controller
  
  def detach(self):
    self.controller = None
    
  def serve_forever(self):
    abort = 0
    while not abort:
      rd, wr, ex = select.select([self.socket.fileno()], [], [], self.timeout)
      if rd:
        self.handle_request()
      abort = self.abort

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
