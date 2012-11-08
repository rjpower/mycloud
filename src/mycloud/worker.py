#!/usr/bin/env python

from rpc.server import RPCServer
import argparse
import cStringIO
import logging
import multiprocessing
import mycloud.thread
import mycloud.util
import os
import select
import socket
import sys
import time

'''Worker for executing cluster tasks.'''

WORKERS = None

def watchdog():
  while 1:
    r, w, x = select.select([sys.stdin], [], [sys.stdin], 10)
    if r or x:
      #logging.debug('Lost controller.  Exiting.')
      if WORKERS is not None:
        WORKERS.terminate()
      sys.exit(1)
    
#    logging.info('Watchdog stacktraces: %s', '\n\t'.join(mycloud.util.stacktraces()))

class WorkerHandler(object):
  def __init__(self, host, port, log_host, log_port):
    self.host = host
    self.port = port
    self.log_host = log_host
    self.log_port = log_port
    self.last_keepalive = time.time()
    self._next_task_id = iter(xrange(1000000))
    self.tasks = {}

    logging.info('Worker started on %s:%s', self.host, self.port)

  def healthcheck(self, handle):
    self.last_keepalive = time.time()
    self.done('alive')
    
  def num_cores(self, handle):
    handle.done(multiprocessing.cpu_count())

  def run(self, handle, f_pickle, a_pickle, kw_pickle):
#    handle.done(mycloud.util.run_task(self.log_host, self.log_port, 
#                                      f_pickle, a_pickle, kw_pickle))
    WORKERS.apply_async(mycloud.util.run_task, 
                        (self.log_host, self.log_port, f_pickle, a_pickle, kw_pickle), 
                        callback=lambda res: handle.done(res))

if __name__ == '__main__':
  WORKERS = multiprocessing.Pool()
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
    mycloud.util.add_socket_logger(opts.logger_host, opts.logger_port)

  mycloud.thread.spawn(watchdog)
  worker = WorkerHandler(socket.gethostname(), myport, 
                         opts.logger_host, opts.logger_port)
  server = RPCServer('0.0.0.0', myport, worker)

  sys.stdout.write('%s\n' % myport)
  sys.stdout.flush()
  
  # capture stdout and stderr locally.  
  # periodically poll them to forward messages # to the master
  mycloud.util.redirect_out_err()

  server.serve_forever()
