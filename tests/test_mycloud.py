#!/usr/bin/env python

import logging
import mycloud
import unittest
from mycloud.mapreduce import MapReduce
from mycloud.resource import CSV  

logging.basicConfig(format='%(asctime)s %(filename)s:%(funcName)s %(message)s',
                    level=logging.INFO)

def bad_func(idx):
  raise Exception('idx %d' % idx)

class TestMycloud(unittest.TestCase):
  def test_mr(self):
    cluster = mycloud.Cluster()    
    for i in range(100):
      w = CSV.Writer('/tmp/my_input_%d.csv' % i)
      for j in range(10):
        w.add(j, j)
      del w
    
    input_desc = [CSV('/tmp/my_input_%d.csv' % i) for i in range(100)]
    output_desc = [CSV('/tmp/my_output.csv')]
   
    def map_identity(k, v, output):
      output(k, int(v[0]))
  
    def reduce_sum(k, values, output):
      output(k, sum(values))
  
    mr = MapReduce(cluster, map_identity, reduce_sum, input_desc, output_desc)
    result = mr.run()
  
    for k, v in result[0].reader():
      print k, v

  def testLocal(self):
    c = mycloud.Cluster(['localhost'])
    self.assertListEqual(
      c.map(lambda a: 2 * a, range(10)),
      map(lambda a: 2 * a, range(10)))
    
  def testLocalLarge(self):
    c = mycloud.Cluster(['localhost'])
    self.assertListEqual(
      c.map(lambda a: 2 * a, range(1000)),
      map(lambda a: 2 * a, range(1000)))

  def testException(self):
    c = mycloud.Cluster(['localhost'])
    self.assertRaises(mycloud.util.ClusterException,
                      lambda: c.map(bad_func, range(10)))
    
  def testDefaultCluster(self):
    c = mycloud.Cluster()
    self.assertListEqual(
      c.map(lambda a: 2 * a, range(10)),
      map(lambda a: 2 * a, range(10)))
  
if __name__ == '__main__':
#  import cProfile
#  tester = TestMap('testLocalLarge')
#  cProfile.runctx('tester.testLocalLarge()', globals(), locals(), 'prof.out')
  unittest.main()
