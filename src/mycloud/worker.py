#!/usr/bin/env python

from rpc.server import RPCServer
import argparse
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

def watchdog():
  while 1:
    r, w, x = select.select([sys.stdin], [], [sys.stdin], 10)
    if r or x:
      #logging.debug('Lost controller.  Exiting.')
      os._exit(1)
    
#    logging.info('Watchdog stacktraces: %s', '\n\t'.join(mycloud.util.stacktraces()))

WORKERS = multiprocessing.Pool()

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

  def run(self, handle, f_pickle, a_pickle, kw_pickle):
#    handle.done(mycloud.util.run_task(self.log_host, self.log_port, 
#                                      f_pickle, a_pickle, kw_pickle))
    WORKERS.apply_async(mycloud.util.run_task, 
                        (self.log_host, self.log_port, f_pickle, a_pickle, kw_pickle), 
                        callback=lambda res: handle.done(res))
        
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
    mycloud.util.add_socket_logger(opts.logger_host, opts.logger_port)

  mycloud.thread.spawn(watchdog)
  worker = WorkerHandler(socket.gethostname(), myport, 
                         opts.logger_host, opts.logger_port)
  server = RPCServer('0.0.0.0', myport, worker)

  sys.stdout.write('%s\n' % myport)
  sys.stdout.flush()
  
  # redirect stdout and stderr to local files to avoid pipe/buffering issues
  # with controller 
  sys.stdout = open(log_prefix + '.out', 'w')
  sys.stderr = open(log_prefix + '.err', 'w')

  server.serve_forever()
