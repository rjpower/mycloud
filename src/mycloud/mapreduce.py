#!/usr/bin/env python

from mycloud.config import OPTIONS
import cPickle
import collections
import leveldb
import logging
import mycloud.thread
import mycloud.util
import os
import rpc.client
import rpc.server
import socket
import sys
import threading
import types

def group(iterator):
  '''Group values in iterator by key.
  
  Assumes the input iterator is sorted.  For each sequence (k1, v1), (k1, v2), (k1, v3)...
  in iterator, yields (k1, [v1, v2, v3]).
'''
  values = []
  last_k = None
  for k, v in iterator:
    if last_k is None:
      last_k = k
      values = [v]
    elif last_k != k:
      yield last_k, values
      last_k = k
      values = [v]
    else:
      values.append(v)
        
  if values:
    yield last_k, values

def shard_for_key(k, num_shards): return hash(k) % num_shards

def identity_mapper(kv_iter, output): 
  for k, v in kv_iter:
    output(k, v)

def identity_reducer(kv_iter, output):
  for k, v in kv_iter:
    output(k, v)

def sum_reducer(kv_iter, output):
  for k, values in group(kv_iter):
    output(k, sum(values))

class MapWorker(object):
  def __init__(self, mapper, index, input, reducers):
    self.mapper = mapper
    self.input = input
    self.index = index
    self.output_tmp = collections.defaultdict(list)
    self.buffer_size = 0
    self.reducers = reducers
    self.num_reducers = len(reducers)
    self.max_map_buffer_size = OPTIONS.max_map_buffer_size

  def target(self, k, v, shard=None):
    if shard is None:
      shard = shard_for_key(k, self.num_reducers)
    sv = cPickle.dumps(v, -1)
    self.output_tmp[shard].append((k, sv))
    self.buffer_size += len(k) + len(sv)

    if self.buffer_size > self.max_map_buffer_size:
      self.flush()

  def flush(self, final=False):
    logging.debug('Flushing map %d', self.index)
    sends = []
    for shard in range(self.num_reducers):
      r = self.reducers[shard]
      shard_output = self.output_tmp[shard]
      if not final and not shard_output:
        continue

      sends.append(r.write_map_output(self.index, shard_output, final))
      
    [s.wait() for s in sends]
    self.output_tmp.clear()
    self.buffer_size = 0
    logging.debug('Flushed map %d', self.index)

  def run(self):
    self.reducers = [rpc.client.RPCClient(host, port) for host, port in self.reducers]
    logging.info('Reading from: %s', self.input)
    if isinstance(self.mapper, types.ClassType):
      mapper = self.mapper(self.mrinfo, self.index, self.input)
    else:
      mapper = self.mapper

    reader = self.input.reader()
    logger = mycloud.util.PeriodicLogger(period=5)
    mapper(reader, self.target)
    self.flush(final=True)
    logging.info('Map of %s finished.', self.input)

class ReduceOutput(object):
  def __init__(self, writer):
    self.writer = writer

  def __call__(self, key, value):
    self.writer.add(key, value)


class ReduceWorker(object):
  def __init__(self, index, target, reducer, num_mappers):
    self.index = index
    self.reducer = reducer
    self.num_mappers = num_mappers
    self.shuffle_tmp = OPTIONS.temp_prefix + '/mycloud-shuffle-tmp-%d' % self.index
    self.maps_finished = [0] * self.num_mappers
    self.target = target

    self.done = False
    self.exc_info = None
    self.server = None
    self.nonce = 0

  def write_map_output(self, handle, mapper, block, is_finished):
    with self.lock:
      logging.debug('Reducer %d - received input from mapper %d', self.index, mapper)
      total_received = 0
      for ser_key, ser_value in block:
        self.nonce += 1
        self.shuffle_db.Put(ser_key + '.%09d' % self.nonce, ser_value)

      if is_finished:
        self.maps_finished[mapper] = 1
        
    handle.done(None)

  def start(self):
    logging.info('Starting server...')
    self.lock = threading.RLock()
    self.port = mycloud.util.find_open_port()
    self.server = rpc.server.RPCServer('0.0.0.0', self.port, self)
    os.system('rm "%s"' % self.shuffle_tmp)
    self.shuffle_db = leveldb.LevelDB(self.shuffle_tmp,
                                      write_buffer_size=OPTIONS.max_reduce_buffer_size,
                                      block_cache_size=(8 * (2 << 20)),
                                      max_open_files=2048,
                                      block_size=128000)

    self.serving_thread = mycloud.thread.spawn(self.server.run)
    self.reducer_thread = mycloud.thread.spawn(self._run)
    
    return (socket.gethostname(), self.port)

  def _run(self):
    try:
      logger = mycloud.util.PeriodicLogger(period=10)
      out = ReduceOutput(self.target.writer())

      while sum(self.maps_finished) != self.num_mappers:
        logger.info('Reducer %d - waiting for map data %d/%d',
                     self.index, sum(self.maps_finished), self.num_mappers)
        mycloud.thread.sleep(1)

      logging.info('Finished reading map data, beginning merge.')

      if not isinstance(self.reducer, types.FunctionType):
        reducer = self.reducer()
      else:
        reducer = self.reducer
        
      def shuffle_iter():
        for k, ser_v in self.shuffle_db.RangeIter():
          v = cPickle.loads(ser_v)
          #logging.info('%s -- %s', k, v)
          # chop off the nonce data
          k = k.rsplit('.', 1)[0]
          yield k, v 
        
      reducer(shuffle_iter(), out)
      del out
      logging.info('Reducer finished - target: %s', self.target)
    except:
      logging.error('Reducer failed!', exc_info=1)
      self.exc_info = sys.exc_info()
    finally:
      self.done = True


  def get_reader(self, handle):
    while not self.done:
      mycloud.thread.sleep(1)

    if self.exc_info is not None:
      logging.error('An error occurred during the execution of the reducer.')
      logging.error(self.exc_info)
      handle.done(self.exc_info)
    else:
      logging.info('Waiting for reducer thread to finish...')
      handle.done(self.target)


class MapReduce(object):
  def __init__(self, controller, mapper, reducer, input, output):
    self.controller = controller
    self.mapper = mapper
    self.reducer = reducer
    self.input = input
    self.target = output

  def run(self):
    logging.info('Inputs: %s...', self.input[:10])
    logging.info('Outputs: %s...', self.target[:10])

    reducers = [ReduceWorker(index=i, target=self.target[i], reducer=self.reducer, num_mappers=len(self.input))
                for i in range(len(self.target)) ]

    reducer_locations = self.controller.map(lambda r: r.start(), reducers)
    print reducer_locations

    mappers = [MapWorker(mapper=self.mapper, index=i, input=self.input[i], reducers=reducer_locations)
               for i in range(len(self.input)) ]

    self.controller.map(lambda m: m.run(), mappers)
    reducer_clients = [rpc.client.RPCClient(host, port) for host, port in reducer_locations]
    return [r.get_reader().wait() for r in reducer_clients]
