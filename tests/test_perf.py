#!/usr/bin/env python

from mycloud.mapreduce import MapReduce
from mycloud.resource import Range, LevelDB
import logging
import mycloud
import mycloud.mapreduce
import unittest

logging.basicConfig(format='%(asctime)s %(filename)s:%(funcName)s %(message)s',
                    level=logging.INFO)

def map_identity(kv_iter, output):
  for k, v in kv_iter: output(str(k), v)

def reduce_identity(kv_iter, output):
  for k, v in kv_iter: output(k, v)

class TestMycloud(unittest.TestCase):
  def test_mr_leveldb(self):
    cluster = mycloud.Cluster() #machines=['localhost'])    
    input_desc = [Range(10000000) for i in range(100)]
    output_desc = [LevelDB('/tmp/my_output.%d.ldb' % i) for i in range (100)]
    result = MapReduce(cluster, map_identity, reduce_identity, input_desc, output_desc).run()

      
if __name__ == '__main__':
#  import cProfile
#  tester = TestMap('testLocalLarge')
#  cProfile.runctx('tester.testLocalLarge()', globals(), locals(), 'prof.out')
  unittest.main()
