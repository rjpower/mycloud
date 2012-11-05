#!/usr/bin/env python

from cloud.serialization import pickledebug as cloudpickle
import Queue
import cPickle
import collections
import logging
import mycloud.connections
import mycloud.thread
import mycloud.util
import random
import socket
import sys
import traceback
import xmlrpclib

mycloud.thread.init()

class ClusterException(Exception):
  pass

def arg_name(args):
  '''Returns a short string representation of an argument list for a task.'''
  a = args[0]
  if isinstance(a, object):
    return a.__class__.__name__
  return repr(a)

class Task(object):
  '''A piece of work to be executed.'''
  def __init__(self, name, index, function, args, kw):
    self.done = False
    self.started = False
    self.idx = index
    logging.debug('Serializing %s %s %s', function, args, kw)
    self.pickle = xmlrpclib.Binary(cloudpickle.dumps((function, args, kw)))
    self.result = None

  def start(self, client):
    self.client = client
    logging.debug('Starting task %s on %s', self.idx, client)
    self.remote_task_id = self.client.start_task(self.pickle)
    self.started = True
  
  def poll(self):
    if not self.started:
      return False
    
    if not self.done:
      finished = self.client.task_done(self.remote_task_id)
      if finished:
        blob = self.client.wait_for_task(self.remote_task_id)
        self.result = cPickle.loads(blob.data) 
        self.done = True
    return self.done
    
  def wait(self):
    while not self.poll():
      mycloud.thread.sleep(0.1)

class Server(object):
  '''Handles connections to remote cores_for_machine and execution of tasks.
  
A Server is created for each core on a machine, and executes tasks as
machine resources become available.'''
  def __init__(self, controller, host, cores):
    self.controller = controller
    self.host = host
    self.cores = cores
    self.slots = [None] * cores

  def ready(self):
    for t in self.slots:
      if t == None or t.done: return True
    return False

  def connect(self):
    ssh = mycloud.connections.SSH.connect(self.host)
    self.stdin, self.stdout, self.stderr = ssh.invoke(
      sys.executable,
      '-m', 'mycloud.worker',
      '--logger_host=%s' % socket.gethostname(),
      '--logger_port=%s' % logging.handlers.DEFAULT_UDP_LOGGING_PORT)

    try:
      self.port = int(self.stdout.readline().strip())
    except:
      logging.error('ERR: %s', self.stderr.read())
      logging.exception('Failed to read port from remote server!')

    self.client = xmlrpclib.ServerProxy('http://%s:%d' % (self.host, self.port))
    
  def start_task(self, task):
    for idx, s in enumerate(self.slots):
      if s is None or s.done:
        self.slots[idx] = task
        task.start(self.client)
        return
    assert False, 'No slots to run task!'
        

class Cluster(object):
  def __init__(self, machines=None, tmp_prefix=None):
    self.cores_for_machine = dict(machines)
    self.tmp_prefix = tmp_prefix
    self.servers = None
    self.exceptions = []

    assert self.cores_for_machine

    self.__start()


  def __start(self):
    self.log_server = mycloud.util.LoggingServer(self)
    mycloud.thread.spawn(self.log_server.serve_forever)

    servers = {}
    index = 0
    for host, cores in self.cores_for_machine.items():
      s = Server(self, host, cores)
      logging.info('Connecting...')
      s.connect()
      servers[host] = s
      index += 1
      logging.info('...done')

    self.servers = servers
    random.shuffle(self.servers)
    logging.info('Started %d servers...', len(servers))

  def __del__(self):
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
    logging.info('Working... %d/%d', tasks_done, len(tasks))

  def map_local(self, f, arglist):
    '''Invoke the given function once for each argument, returning the result
    of the invocations
    
    The function will be run locally on the controller.'''
    class LocalTask(object):
      def __init__(self, f, args, kw):
        self.f = f
        self.args = args
        self.kw = kw
        self.done = False

      def run(self):
        self.result = self.f(*self.args, **self.kw)
        self.done = True

    arglist = mycloud.util.to_tuple(arglist)
    tasks = [LocalTask(f, args, {}) for args in arglist]

    def task_runner():
      for t in tasks:
        t.run()

    runner = mycloud.thread.spawn(task_runner)
    while runner.isAlive():
      self.show_status(tasks)
      mycloud.thread.sleep(1)

  def map(self, f, arglist, name='generic-map'):
    logging.info('Mapping %s over %d inputs', f, len(arglist))
    assert len(arglist) > 0
    idx = 0

    arglist = mycloud.util.to_tuple(arglist)
    task_queue = Queue.Queue()
    tasks = [Task(name, i, f, args, {}) for i, args in enumerate(arglist)]
    for t in tasks: task_queue.put(t)

    logging.info('Mapping %d tasks against %d servers', len(tasks), len(self.servers))

    # Instead of joining on the task_queue, we poll the server 
    # threads so we can stop early in case we encounter an exception.
    try:
      while 1:
        self.check_exceptions()
        self.check_status(tasks)

        for s in self.servers.values():
          while s.ready():
            t = task_queue.get_nowait()
            s.start_task(t)

    except Queue.Empty:
      pass

    for t in tasks:
      while not t.done:
        self.check_status(tasks)
        self.check_exceptions()
        mycloud.thread.sleep(1)

    logging.info('Done.')
    return [t.result for t in tasks]
