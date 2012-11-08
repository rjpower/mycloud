#!/usr/bin/env python

from rpc.server import RPCServer
import argparse
import atexit
import multiprocessing
import mycloud.thread
import mycloud.util
import sys
import time

'''Worker for executing cluster tasks.'''

WORKERS = None

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

  def run(self, handle, f_pickle, a_pickle, kw_pickle):
    WORKERS.apply_async(mycloud.util.run_task, (f_pickle, a_pickle, kw_pickle), callback=lambda res: handle.done(res))

def shutdown():
  print >>sys.stderr, 'Terminating all workers' 
  WORKERS.terminate()
  print >>sys.stderr, '...done'

if __name__ == '__main__':
  p = argparse.ArgumentParser()
  p.add_argument('--logger_host', type=str)
  p.add_argument('--logger_port', type=int)
  p.add_argument('--worker_name', type=str, default='worker')

  opts = p.parse_args()

  myport = mycloud.util.find_open_port()
  server = RPCServer('0.0.0.0', myport, WorkerHandler())
  
  WORKERS = multiprocessing.Pool(initializer = mycloud.util.setup_worker_process, 
                                 initargs=(opts.logger_host, opts.logger_port))


  atexit.register(shutdown)
  mycloud.thread.spawn(mycloud.util.watchdog, sys.stdin)

  sys.stdout.write('%s\n' % myport)
  sys.stdout.flush()

  mycloud.util.setup_worker_process(opts.logger_host, opts.logger_port)
  server.serve_forever()
