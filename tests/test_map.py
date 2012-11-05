#!/usr/bin/env python

import mycloud
import unittest

class TestMap(unittest.TestCase):
  def testLocal(self):
    c = mycloud.Cluster([('localhost', 4)])
    self.assertListEqual(
      c.map(lambda a: 2 * a, range(10)),
      map(lambda a: 2 * a, range(10)))
    
  def testLocalLarge(self):
    c = mycloud.Cluster([('localhost', 100)])
    self.assertListEqual(
      c.map(lambda a: 2 * a, range(10000)),
      map(lambda a: 2 * a, range(10000)))
            
if __name__ == '__main__':
    unittest.main()
