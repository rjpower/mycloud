#!/usr/bin/env python

from mycloud.config import OPTIONS
from rpc.server import RPCServer
import cPickle
import logging
import multiprocessing
import mycloud.util
import os
import select
import sys
import tempfile
import threading
import time
import traceback


'''Worker for executing cluster tasks.'''

WORKERS = None

class WorkerException(object):
  def __init__(self, exc_info):
    tb = traceback.format_exception(*exc_info)
    tb = '\n'.join(tb)
    self.tb = tb.replace('\n', '\n>> ')


def watchdog():
  while 1:
    r, w, x = select.select([sys.stdin], [], [sys.stdin], 10)
    if r or x:
      # logging.debug('Lost controller.  Exiting.')
      if WORKERS is not None:
        try:
          WORKERS.terminate()
        except:
          pass
      os._exit(1)
    
#    logging.info('Watchdog stacktraces: %s', '\n\t'.join(mycloud.util.stacktraces()))

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

def setup_worker_process(log_host, log_port):
  redirect_out_err()
  setup_remote_logging(log_host, log_port)  

def run_task(f_pickle, a_pickle, kw_pickle):
  try:
#    logging.info('Starting task!!!')
    function = cPickle.loads(f_pickle)
    args = cPickle.loads(a_pickle)
    kw = cPickle.loads(kw_pickle)
    # logging.info('S')
    result = function(*args, **kw)
    # logging.info('E')
    return result
  except:
    logging.info('WORKER: failed to execute task.', exc_info=1)
    try:
      return WorkerException(sys.exc_info())
    except:
      logging.error('WTF', exc_info=1)
      

class WorkerHandler(object):
  def __init__(self):
    self.last_keepalive = time.time()
    self._next_task_id = iter(xrange(1000000))
    self.tasks = {}

  def healthcheck(self, handle):
    self.last_keepalive = time.time()
    self.done('alive')
    
  def num_cores(self, handle):
    handle.done(multiprocessing.cpu_count())
    
  def setup(self, handle, opt):
    logging.info('Setting up worker with new options...')
    for k in OPTIONS.__slots__:
      setattr(OPTIONS, k, getattr(opt, k))
      logging.info('Updating option: %s, old: %s, new: %s', k, getattr(OPTIONS, k), getattr(opt, k))
      
    setup_worker_process(OPTIONS.log_host, OPTIONS.log_port)
    
    global WORKERS
    WORKERS = multiprocessing.Pool(initializer=setup_worker_process, initargs=(OPTIONS.log_host, OPTIONS.log_port))
    handle.done(True)

  def run(self, handle, f_pickle, a_pickle, kw_pickle):
    # handle.done(run_task(f_pickle, a_pickle, kw_pickle))
    WORKERS.apply_async(run_task, (f_pickle, a_pickle, kw_pickle),
                        callback=lambda res: handle.done(res))

def run_worker():
  myport = mycloud.util.find_open_port()
  sys.stdout.write('%s\n' % myport)
  sys.stdout.flush()

  t = threading.Thread(target=watchdog, args=sys.stdin)
  t.setDaemon(True)
  t.start()

  server = RPCServer('0.0.0.0', myport, WorkerHandler())
  server.serve_forever()
