#!/usr/bin/env python

import logging
import mycloud
import unittest

logging.basicConfig(format='%(asctime)s %(filename)s:%(funcName)s %(message)s',
                    level=logging.INFO)

def bad_func(idx):
  raise Exception('idx %d' % idx)

class TestMap(unittest.TestCase):
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
