#!/usr/bin/env python

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SocketServer import UDPServer, ThreadingMixIn
from cloud.serialization import cloudpickle
import cPickle
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

class LoggingServer(UDPServer):
  '''Listen for UDP log messages on the default port.
  
If a record containing an exception is found, report it to the controller.
'''
  log_output = ""

  def __init__(self, controller):
    host = '0.0.0.0'
    port = logging.handlers.DEFAULT_UDP_LOGGING_PORT

    UDPServer.__init__(self, (host, port), None)
    self.timeout = 0.1
    self.controller = controller

    # for each distinct host, keep track of the last message sent
    self.message_map = {}

  def server_bind(self):
    logging.info('LoggingServer binding to address %s', self.server_address)
    self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    UDPServer.server_bind(self)

  def finish_request(self, request, client_address):
    packet, socket = request

    rlen = struct.unpack('>L', packet[:4])[0]

    if len(packet) != rlen + 4:
      logging.error('Received invalid logging packet. %s %s',
                    len(packet), rlen)

    record = logging.makeLogRecord(cPickle.loads(packet[4:]))
    srchost = client_address[0]

    self.message_map[client_address] = record

    if record.exc_info:
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

class XMLServer(ThreadingMixIn, SimpleXMLRPCServer):
  def __init__(self, *args, **kw):
    SimpleXMLRPCServer.__init__(self, *args, **kw)
    self.daemon_threads = True

  def _dispatch(self, method, params):
    try:
      return getattr(self, method)(*params)
    except:
      logging.info('Exception during dispatch.', exc_info=1)
      raise xmlrpclib.Fault(faultCode=1, faultString=traceback.format_exc())

  def server_bind(self):
    logging.info('Binding to address %s', self.server_address)
    self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    SimpleXMLRPCServer.server_bind(self)


class ProxyServer(XMLServer):
  def __init__(self):
    self.wrapped_objects = {}
    XMLServer.__init__(self, ('0.0.0.0', find_open_port()))

  def wrap(self, obj):
    self.wrapped_objects[id(obj)] = obj
    #logging.info('Wrapped object %s', id(obj))
    return ProxyObject(socket.gethostname(), self.server_address[1], id(obj))

  def invoke(self, objid, method, *args, **kw):
    try:
      #logging.info('Invoking method: %s', method)
      result = getattr(self.wrapped_objects[objid], method)(*args, **kw)
      #logging.info('Successfully invoked %s', method)
      return xmlrpclib.Binary(cloudpickle.dumps(result))
    except:
      logging.info('Exception during dispatch of %s.', method, exc_info=1)
      raise


class ProxyObject(object):
  def __init__(self, host, port, objid):
    self.host = host
    self.port = port
    self.objid = objid
    self.server = None

  def get_server(self):
    if self.server is None:
#      logging.info('Connecting to %s %d', self.host, self.port)
      self.server = xmlrpclib.ServerProxy('http://%s:%d' % (self.host, self.port))
#      logging.info('Connection established to %s %d', self.host, self.port)
    return self.server

  def invoke(self, method, *args, **kw):
    for i in range(5):
      try:
        result = self.get_server().invoke(self.objid, method, *args, **kw)
        return cPickle.loads(result.data)
      except:
        logging.info('Failed to invoke remote method %s on %s.  Trying again.',
                     method, self.host, exc_info=1)
        mycloud.thread.sleep(5)
    raise Exception('Failed to invoke remote method %s on %s' %
                    (method, self.host))
