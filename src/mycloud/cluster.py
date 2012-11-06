#!/usr/bin/env python

from cloud.serialization import cloudpickle
from rpc.client import RPCClient
import Queue
import collections
import logging
import mycloud.connections
import mycloud.thread
import mycloud.util
import socket
import sys
import traceback
from mycloud.util import PeriodicLogger
    
LOG_SERVER = None

def start_logserver():
  global LOG_SERVER
  if LOG_SERVER is not None:
    return

  LOG_SERVER = mycloud.util.LoggingServer()
  mycloud.thread.spawn(LOG_SERVER.serve_forever)

class ClusterException(Exception):
  pass

@mycloud.util.memoized
def cached_pickle(v):
  return cloudpickle.dumps(v)

class Task(object):
  '''A piece of work to be executed.'''
  def __init__(self, name, index, function, args, kw):
    self.started = False
    self.idx = index
    logging.debug('Serializing %s %s %s', function, args, kw)
    
    # Cache pickled functions, since they are the same for every task
    self.f_pickle = cached_pickle(function)
    self.args_pickle = cloudpickle.dumps(args)
    self.kw_pickle = cloudpickle.dumps(kw)
    
    self.future = None

  def start(self, client):
    self.client = client
    logging.debug('Starting task %s on %s', self.idx, client)
    self.future = self.client.run(self.f_pickle, self.args_pickle, self.kw_pickle)
    self.started = True
  
  def poll(self):
    if self.future and self.future.done():
      return True
    return False
  
  def wait(self):
    return self.future.wait()

class Server(object):
  '''Handles connections to remote cores_for_machine and execution of tasks.
  
A Server is created for each core on a machine, and executes tasks as
machine resources become available.'''
  def __init__(self, controller, host, cores):
    self.controller = controller
    self.host = host
    self.cores = cores
    self.tasks = []

  def run(self, task):
    task.start(self.client)
    self.tasks.append(task)
  
  def idle_cores(self):
    self.tasks = [t for t in self.tasks if not t.poll()]
    return self.cores - len(self.tasks)
  
  def connect(self):
    ssh = mycloud.connections.SSH.connect(self.host)
    self.stdin, self.stdout, self.stderr = ssh.invoke(
      sys.executable,
      '-m', 'mycloud.worker',
      '--logger_host=%s' % socket.gethostname(),
      '--logger_port=%s' % logging.handlers.DEFAULT_TCP_LOGGING_PORT)

    try:
      self.port = int(self.stdout.readline().strip())
    except:
      logging.error('ERR: %s', self.stderr.read())
      logging.exception('Failed to read port from remote server!')

    self.client = RPCClient(self.host, self.port)
    

class Cluster(object):
  def __init__(self, machines=None, tmp_prefix=None):
    self.cores_for_machine = dict(machines)
    self.tmp_prefix = tmp_prefix
    self.servers = None
    self.exceptions = []
    self.status_logger = PeriodicLogger(5)

    start_logserver()
    LOG_SERVER.attach(self)

    servers = {}
    connections = []
    index = 0
    for host, cores in self.cores_for_machine.items():
      s = Server(self, host, cores)
      servers[host] = s
      connections.append(mycloud.thread.spawn(s.connect))

    for c in connections: c.join()

    self.servers = servers
    logging.info('Started %d servers...', len(servers))

  def __del__(self):
    LOG_SERVER.detach()
    logging.info('Goodbye!')

  def report_exception(self, exc):
    self.exceptions.append(exc)

  def check_exceptions(self):
    '''Check if any remote exceptions have been thrown.  Log locally and rethrow.

If an exception is found, the controller is shutdown and all exceptions are reported
prior to raising a ClusterException.'''
    if self.exceptions:
      mycloud.connections.SSH.shutdown()

      counts = collections.defaultdict(int)

      for e in self.exceptions:
        exc_dump = '\n'.join(traceback.format_exception(*e))
        counts[exc_dump] += 1

      for exc_dump, count in sorted(counts.items(), key=lambda t: t[1]):
        logging.info('Remote exception (occurred %d times):' % count)
        logging.info('%s', '\nREMOTE:'.join(exc_dump.split('\n')))

      raise ClusterException

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

    logging.info('Mapping %d tasks against %d servers', len(tasks), len(self.servers))
    
    done = False
    while not done:
      if not task_queue.empty():
        try:
          for server in self.servers.values():
            for core in range(server.idle_cores()):
              t = task_queue.get_nowait()
              server.run(t)
        except Queue.Empty:
          pass

      done = self.check_status(tasks)
      self.check_exceptions()
      mycloud.thread.sleep(0.01)

    logging.info('Done.')
    return [t.wait() for t in tasks]
