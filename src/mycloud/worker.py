#!/usr/bin/env python

from cloud.serialization import cloudpickle
from rpc.server import RPCServer
import argparse
import cPickle
import logging
import mycloud.thread
import mycloud.util
import os
import select
import socket
import sys
import threading
import time

'''Worker for executing cluster tasks.'''

def watchdog():
  while 1:
    r, w, x = select.select([sys.stdin], [], [sys.stdin], 10)
    if r or x:
      #logging.debug('Lost controller.  Exiting.')
      os._exit(1)
    
#    logging.info('Watchdog stacktraces: %s', '\n\t'.join(mycloud.util.stacktraces()))

class WorkerTask(threading.Thread):
  def __init__(self, handle, f_pickle, a_pickle, kw_pickle):
    threading.Thread.__init__(self)
    self.handle = handle
    self.function = cPickle.loads(f_pickle)
    self.args = cPickle.loads(a_pickle)
    self.kw = cPickle.loads(kw_pickle)
  
  def run(self):
    try:
      self.result = self.function(*self.args, **self.kw)
    except:
      logging.info('Failed to execute task.', exc_info=1)
      self.result = sys.exc_info()
    
    self.handle.done(self.result)
  
class WorkerHandler(object):
  def __init__(self, host, port):
    self.host = host
    self.port = port
    self.last_keepalive = time.time()
    self._next_task_id = iter(xrange(1000000))
    self.tasks = {}

    logging.info('Worker started on %s:%s', self.host, self.port)

  def run(self, handle, f_pickle, a_pickle, kw_pickle):
    self.cleanup()
    w = WorkerTask(handle, f_pickle, a_pickle, kw_pickle)
    w.start()
    self.tasks[id(w)] = w
    
  def cleanup(self):
    for tid, t in self.tasks.items():
      if not t.isAlive():
        t.join()
        del self.tasks[tid]
    
  def healthcheck(self, handle):
    self.last_keepalive = time.time()
    self.done('alive')

if __name__ == '__main__':
  p = argparse.ArgumentParser()
  p.add_argument('--logger_host', type=str)
  p.add_argument('--logger_port', type=int)
  p.add_argument('--worker_name', type=str, default='worker')

  opts = p.parse_args()

  myport = mycloud.util.find_open_port()

  log_prefix = '/tmp/%s-worker' % socket.gethostname()

  logging.basicConfig(stream=open(log_prefix + '.log', 'w'),
                      format='%(asctime)s %(filename)s:%(funcName)s %(message)s',
                      level=logging.INFO)

  if opts.logger_host:
    logging.info('Additionally logging to %s:%s',
                 opts.logger_host, opts.logger_port)

    logging.getLogger().addHandler(
      logging.handlers.DatagramHandler(opts.logger_host, opts.logger_port))

  worker = WorkerHandler(socket.gethostname(), myport)
  server = RPCServer('0.0.0.0', myport, worker)

  sys.stdout.write('%s\n' % myport)
  sys.stdout.flush()
  
  # redirect stdout and stderr to local files to avoid pipe/buffering issues
  # with controller 
  sys.stdout = open(log_prefix + '.out', 'w')
  sys.stderr = open(log_prefix + '.err', 'w')

  mycloud.thread.spawn(watchdog)
  server.serve_forever()