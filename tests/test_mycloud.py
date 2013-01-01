#!/usr/bin/env python

from mycloud.mapreduce import MapReduce
from mycloud.resource import CSV, LevelDB
import logging
import os
import mycloud
import mycloud.mapreduce
import unittest

logging.basicConfig(format='%(asctime)s %(filename)s:%(funcName)s %(message)s',
                    level=logging.INFO)

def bad_func(idx):
  raise Exception('idx %d' % idx)

  
def map_identity(kv_iter, output):
  for k, v in kv_iter:
    output(k, int(v[0]))

def reduce_sum(kv_iter, output):
  logging.info('%s', kv_iter)
  for k, values in mycloud.mapreduce.group(kv_iter):
    logging.info('%s %s', k, values)
    output(k, sum(values))

def write_csv_files():
  for i in range(10):
    w = CSV.Writer('/tmp/my_input_%d.csv' % i)
    for j in range(10):
      w.add(j, j)
    del w    

class TestMycloud(unittest.TestCase):
  def test_local(self):
    c = mycloud.Cluster(machines=['localhost'])
    self.assertListEqual(
      c.map(lambda a: 2 * a, range(10)),
      map(lambda a: 2 * a, range(10)))
    
  def test_map_large(self):
    c = mycloud.Cluster(machines=['localhost'])
    self.assertListEqual(
      c.map(lambda a: 2 * a, range(100)),
      map(lambda a: 2 * a, range(100)))

  def test_exception(self):
    c = mycloud.Cluster(machines=['localhost'])
    self.assertRaises(mycloud.cluster.ClusterException,
                      lambda: c.map(bad_func, range(10)))

class TestMR(unittest.TestCase):
  def test_mr_csv(self):
    write_csv_files()
    cluster = mycloud.Cluster(machines=['localhost'])    
    input_desc = [CSV('/tmp/my_input_%d.csv' % i) for i in range(10)]
    output_desc = [CSV('/tmp/my_output.csv')]

    result = MapReduce(cluster, map_identity, reduce_sum, input_desc, output_desc).run()
    for k, v in result[0].reader():
      print k, v

  def test_mr_leveldb(self):
    write_csv_files()
    cluster = mycloud.Cluster(machines=['localhost'])    
    input_desc = [CSV('/tmp/my_input_%d.csv' % i) for i in range(10)]
    output_desc = [LevelDB('/tmp/my_output.ldb')]
    
    result = MapReduce(cluster, map_identity, reduce_sum, input_desc, output_desc).run()
    for k, v in result[0].reader():
      print k, v
    

class TestClientFS(unittest.TestCase):
  def test_cloud_read(self):
    c = mycloud.Cluster(machines=['localhost'])
    os.system('rm -rf /tmp/foo')
    rf = mycloud.fs.FS.open('client:///tmp/foo', 'w')
    rf.write('hello!')
    rf.close()
    
    lf = open('/tmp/foo').read()
    self.assertEqual(lf, 'hello!')
    
  def test_cloud_iter(self):
    c = mycloud.Cluster(machines=['localhost'])
    os.system('rm -rf /tmp/foo')
    lf = open('/tmp/foo', 'w')
    lf.write('abcabcabc\n' * 1000)
    lf.close()
    rf = mycloud.fs.FS.open('client:///tmp/foo', 'r')

    llines = list(open('/tmp/foo'))
    rlines = list(rf)
    
    self.assertListEqual(llines, rlines)
    
  
  def test_mr_cloud_fs(self):
    write_csv_files()
    cluster = mycloud.Cluster(machines=['localhost'])    
    input_desc = [CSV('client:///tmp/my_input_%d.csv' % i) for i in range(10)]
    output_desc = [CSV('client:///tmp/my_output.csv')]

    result = MapReduce(cluster, map_identity, reduce_sum, input_desc, output_desc).run()
    for k, v in result[0].reader():
      print k, v

      
if __name__ == '__main__':
  unittest.main()
