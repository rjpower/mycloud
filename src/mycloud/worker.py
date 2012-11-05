#!/usr/bin/env python

from cloud.serialization import cloudpickle
from mycloud.util import XMLServer
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
import xmlrpclib

mycloud.thread.init()

__doc__ = '''Worker for executing cluster tasks.'''

def watchdog(worker):
  while 1:
    r, w, x = select.select([sys.stdin], [], [sys.stdin], 10)
    if r or x:
      #logging.debug('Lost controller.  Exiting.')
      os._exit(1)

#    logging.info('Watchdog stacktraces: %s',
#                 '\n\t'.join(mycloud.util.stacktraces()))

class WorkerTask(threading.Thread):
  def __init__(self, pickled):
    threading.Thread.__init__(self)
    self.pickled_data = pickled.data
  
  def run(self):
    try:
      f, args, kw = cPickle.loads(self.pickled_data)
      logging.info('Executing task %s %s %s', f, args, kw)
      self.result = f(*args, **kw)
    except:
      logging.info('Failed to execute task.', exc_info=1)
      self.result = sys.exc_info()
  
class Worker(XMLServer):
  def __init__(self, *args, **kw):
    XMLServer.__init__(self, *args, **kw)

    self.host = socket.gethostname()
    self.port = self.server_address[1]
    self.last_keepalive = time.time()
    self.tasks = {}

    logging.info('Worker started on %s:%s', self.host, self.port)

  def start_task(self, pickled):
    t = WorkerTask(pickled)
    self.tasks[id(t)] = t
    t.start()
    return id(t)
  
  def task_done(self, tid):
    return not self.tasks[tid].isAlive()
    
  def wait_for_task(self, tid):
    task = self.tasks[tid]
    task.join()
    return xmlrpclib.Binary(cPickle.dumps(task.result, -1))

  
  def execute_task(self, pickled):
    task_id = self.start_task(pickled)
    return self.wait_for_task(task_id)

  def healthcheck(self):
    self.last_keepalive = time.time()
    return 'alive'

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

  worker = Worker(('0.0.0.0', myport))
  worker.timeout = 1

  sys.stdout.write('%s\n' % myport)
  sys.stdout.flush()
  
  # redirect stdout and stderr to local files to avoid pipe/buffering issues
  # with controller 
  sys.stdout = open(log_prefix + '.out', 'w')
  sys.stderr = open(log_prefix + '.err', 'w')

  mycloud.thread.spawn(watchdog, worker)

  # handle requests until we lose our stdin connection the controller
  try:
    while 1:
      worker.handle_request()
  except:
    logging.info('Error while serving.', exc_info=1)


  logging.info('Shutting down.')
