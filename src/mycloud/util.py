#!/usr/bin/env python

from SocketServer import TCPServer
import cPickle
import collections
import functools
import logging
import mycloud.thread
import os
import select
import socket
import struct
import sys
import tempfile
import threading
import time
import traceback
import types

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


def create_tempfile(dir, suffix):
  os.system("mkdir -p '%s'" % dir)
  return tempfile.NamedTemporaryFile(dir=dir, suffix=suffix)

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

def setup_remote_logging(host, port):
  '''Reset the logging configuration, and set all logging to go through the remote socket logger.'''
  import logging.handlers
  formatter = logging.Formatter('%(asctime)s %(filename)s:%(funcName)s %(message)s', None)

  sock_handler = logging.handlers.SocketHandler(host, port)
  sock_handler.setFormatter(formatter)
  
  err_handler = logging.StreamHandler(sys.stderr)
  err_handler.setFormatter(formatter)
  
  root = logging.getLogger()
  root.setLevel(logging.INFO)
  root.handlers = [sock_handler, err_handler]

def redirect_out_err():
  td = tempfile.gettempdir()
  sys.stdout = open('%s/mycloud.worker.out.%d' % (td, os.getpid()), 'w')
  sys.stderr = open('%s/mycloud.worker.err.%d' % (td, os.getpid()), 'w')
  # TODO(power) -- spawn a thread to monitor these and dump into the logger

def set_non_blocking(f):
  import fcntl
  flags = fcntl.fcntl(f, fcntl.F_GETFL, 0)
  fcntl.fcntl(f, fcntl.F_SETFL, flags | os.O_NONBLOCK)
  
def setup_worker_process(log_host, log_port):
  redirect_out_err()
  setup_remote_logging(log_host, log_port)  

# multiprocessing doesn't work with functions defined in the __main__ module, otherwise
# this would be in worker.py
def run_task(f_pickle, a_pickle, kw_pickle):
  try:
#    logging.info('Starting task!!!')
    function = cPickle.loads(f_pickle)
    args = cPickle.loads(a_pickle)
    kw = cPickle.loads(kw_pickle)
    return function(*args, **kw)
  except:
    logging.info('Failed to execute task.', exc_info=1)
    return WorkerException(traceback.format_exception(*sys.exc_info()))
      
class ClusterException(Exception):
  pass

class WorkerException(object):
  def __init__(self, tb):
    self.tb = tb

class LoggingServer(TCPServer):
  '''Listens on the local TCP logging port and forwards remote log messages
  (sent using logging.handlers.SocketHandler) to the local logging system.
  
  Exceptions are passed onto the local 'controller' object specified with the 
  attach method. 
  '''   
  def __init__(self):
    host = '0.0.0.0'
    port = logging.handlers.DEFAULT_TCP_LOGGING_PORT
    self.allow_reuse_address = True
    TCPServer.__init__(self, (host, port), None)
    self.timeout = 0.1
    self.controller = None

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
        logging.info('Bad log message from %s', socket.client_address)
        return
      req += chunk

    record = logging.makeLogRecord(cPickle.loads(req))
    if record.exc_info and self.controller is not None:
      self.controller.report_exception(record.exc_info)
    else:
      logging.getLogger().handle(record)

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
