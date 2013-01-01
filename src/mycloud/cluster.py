#!/usr/bin/env python

from cloud.serialization import cloudpickle
from mycloud.config import OPTIONS
from rpc.client import RPCClient
import Queue
import collections
import logging
import mycloud.connections
import mycloud.fs
import mycloud.thread
import mycloud.worker
import mycloud.util
import rpc.server
import socket
import sys
import traceback

FILE_SERVER = None    
LOG_SERVER = None

class ClusterException(Exception):
  pass

def start_helper_servers():
  global LOG_SERVER, FILE_SERVER
  if LOG_SERVER is not None: return

  LOG_SERVER = mycloud.util.LoggingServer()
  mycloud.thread.spawn(LOG_SERVER.serve_forever)
  
  fs_port = mycloud.util.find_open_port()
  FILE_SERVER = rpc.server.RPCServer('0.0.0.0', fs_port, mycloud.fs.ClientFSHandler())
  mycloud.thread.spawn(FILE_SERVER.serve_forever)
  
  OPTIONS.fs_host = socket.gethostname()
  OPTIONS.fs_port = fs_port
  logging.info('Setting fs_host=%s, fs_port=%s', OPTIONS.fs_host, OPTIONS.fs_port)
  OPTIONS.log_host = socket.gethostname()
  
@mycloud.util.memoized
def cached_pickle(v):
  return cloudpickle.dumps(v)

class Task(object):
  def __init__(self, name, index, function, args, kw):
    self.started = False
    self.idx = index
    logging.debug('Serializing %s %s %s', function, args, kw)
    
    # Cache pickled functions, since they are the same for every task
    try:
      self.f_pickle = cached_pickle(function)
      self.args_pickle = cloudpickle.dumps(args)
      self.kw_pickle = cloudpickle.dumps(kw)
    except:
      logging.info('Failed to pickle: %s %s %s', function, args, kw)
      raise
    
    self.future = None

  def start(self, client):
    self.client = client
    logging.debug('Starting task %s on %s', self.idx, client)
    self.future = self.client.run(self.f_pickle, self.args_pickle, self.kw_pickle)
    self.started = True
  
  def poll(self):
    if self.done():
      result = self.future.result
      if isinstance(result, mycloud.worker.WorkerException):
        raise ClusterException, '\n'.join(result.tb).replace('\n', '\n>> ')
      return True
    return False
  
  def done(self):
    return self.future is not None and self.future.done()
  
  def wait(self):
    result = self.future.wait()
    if isinstance(result, mycloud.worker.WorkerException):
      raise ClusterException, '\n'.join(result.tb).replace('\n', '\n>> ')
    return result

class WorkerClient(object):
  '''Manages connections and task execution for a remote machine.'''
  def __init__(self, controller, host):
    self.controller = controller
    self.host = host
    self.tasks = []
    self.cores = -1

  def run(self, task):
    task.start(self.client)
    self.tasks.append(task)
  
  def idle_cores(self):
    return self.cores - len(self.tasks)
  
  def poll(self):
    self.tasks = [t for t in self.tasks if not t.done()]
  
  def connect(self):
    logging.info('host: %s', self.host)
    ssh = mycloud.connections.Local.connect(self.host)
    stdin, stdout, stderr = ssh.invoke(
      sys.executable,
      '-m', 'mycloud.worker_main',
      '--logger_host=%s' % socket.gethostname(),
      '--logger_port=%s' % logging.handlers.DEFAULT_TCP_LOGGING_PORT)
    
    try:
      self.port = int(stdout.readline().strip())
    except:
      logging.error('ERR: %s', stderr.read())
      logging.exception('Failed to read port from remote server!')

    self.client = RPCClient(self.host, self.port)
    self.client.setup(OPTIONS).wait()
    self.cores = self.client.num_cores().wait()

class Cluster(object):
  def __init__(self, **kw):
    for k, v in kw.items():
      setattr(OPTIONS, k, v)
    
    start_helper_servers()
    self.servers = None
    self.exceptions = []
    self.status_logger = mycloud.util.PeriodicLogger(5)

    LOG_SERVER.attach(self)

    # to speed up initializing, spawn and connect to all of our remote
    # servers in parallel
    servers = {}
    connections = []
    index = 0
    for host in OPTIONS.machines:
      s = WorkerClient(self, host)
      servers[host] = s
      connections.append(mycloud.thread.spawn(s.connect))

    for c in connections: c.join()

    self.servers = servers
    logging.info('Started %d servers...', len(servers))

  def __del__(self):
    if LOG_SERVER is not None:
      LOG_SERVER.detach()
    logging.info('Goodbye!')

  def check_status(self, tasks):
    tasks_done = sum([t.poll() for t in tasks])
    self.status_logger.info('Working... %d/%d', tasks_done, len(tasks))
    
    return tasks_done == len(tasks)

  def map_local(self, f, arglist):
    '''Invoke the given function once for each argument, returning the future
    of the invocations
    
    The function will be run locally on the controller.'''
    class LocalTask(object):
      def __init__(self, f, args, kw):
        self.f = f
        self.args = args
        self.kw = kw
        self.done = False

      def run(self):
        self.future = self.f(*self.args, **self.kw)
        self.done = True

    arglist = mycloud.util.to_tuple(arglist)
    tasks = [LocalTask(f, args, {}) for args in arglist]

    for t in tasks:
      t.run()
  
  def map(self, f, arglist, name='generic-map'):
    assert len(arglist) > 0
    idx = 0

    arglist = mycloud.util.to_tuple(arglist)
    task_queue = Queue.Queue()
    logging.info('Serializing %d inputs', len(arglist))
    tasks = []
    for idx, args in enumerate(arglist):
      t = Task(name, idx, f, args, {})
      tasks.append(t)
      task_queue.put(t)

    logging.info('Mapping %d tasks against %d servers',
                 len(tasks), len(self.servers))
    
    while not task_queue.empty():
      for _ in range(8):
        for server in self.servers.values():
          if server.idle_cores() > 0 and not task_queue.empty():
            server.run(task_queue.get_nowait())

      self.check_status(tasks)
      for server in self.servers.values(): 
        server.poll()
      
    while not self.check_status(tasks):
      mycloud.thread.sleep(0.1)

    logging.info('Done.')
    results = [t.wait() for t in tasks]
    found_exc = False
    for r in results:
      if isinstance(r, mycloud.worker.WorkerException):
        found_exc = True
        logging.error('Exception on worker: %s', r.tb)
    
    if found_exc:
      raise ClusterException('Failing due to errors during map.')
    return results
