#!/usr/bin/env python

'''An efficient merger of sorted on-disk tables.

Usage:

for k, v in Merger(shard_list):
  output.add(key, sum(values))

'''

from blocked_table import Table
from heapq import heappop, heappush, heapify
import logging

class HeapHelper(object):
  def __init__(self, d):
    self.iter = iter(d)

  def __cmp__(self, b):
    return cmp(self.key, b.key)

  def next(self):
    try:
      self.key, self.value = self.iter.next()
    except StopIteration:
      self.key = self.value = None
      return None
    return (self.key, self.value)


class Merger(object):
  def __init__(self, shards):
    self.shards = shards

  def __iter__(self):
    helpers = [HeapHelper(f) for f in self.shards]

    # prime the heap
    helpers = [h for h in helpers if h.next()]
    heapify(helpers)

    count = 0
    values = []
    while helpers:
      count += 1
      top = helpers[0].key
      if count % 10000 == 0:
        logging.info('(%6d) -- merging: %s', count, top)

      while helpers and helpers[0].key == top:
        h = heappop(helpers)
        values.append(h.value)
        if h.next() is not None:
          heappush(helpers, h)

      yield top, values
      del values[:]
