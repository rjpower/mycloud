MyCloud
===================

Leverage small clusters of machines to increase your productivity.

MyCloud requires no prior setup; if you can SSH to your machines, then it 
will work out of the box.  MyCloud currently exports a simple mapreduce API 
with several common input formats; adding support for your own is easy as well.

Usage
=====

Starting your cluster:

    import mycloud
 
    cluster = mycloud.Cluster(['machine1', 'machine2'])
     
    # or use defaults from ~/.config/mycloud
    # cluster = mycloud.Cluster()
    
Map over a list:
 
    result = cluster.map(compute_factors, range(1000))

ClientFS makes accessing local files seamless!

    def my_worker(filename):
      do_work(mycloud.fs.FS.open(filename, 'r'))
    
    cluster.map(['client:///my/local/file'], my_worker)

Use the MapReduce interface to easily handle processing of larger datasets:
  
    from mycloud.mapreduce import MapReduce, group
    from mycloud.resource import CSV  
    input_desc = [CSV('client:///path/to/my_input_%d.csv') % i for i in range(100)]
    output_desc = [CSV('client:///path/to/my_output_file.csv')]
   
    def map_identity(kv_iter, output):
      for k, v in kv_iter:
        output(k, int(v[0]))
  
    def reduce_sum(kv_iter, output): 
      for k, values in group(kv_iter):
        output(k, sum(values))
  
    mr = MapReduce(cluster, map_identity, reduce_sum, input_desc, output_desc)
  
    result = mr.run()
  
    for k, v in result[0].reader():
      print k, v

Performance
===========

It is, keep in mind, written entirely in Python.

Some simple operations I've used it for (6 machines, 96 cores):

  * Sorting a billion numbers: ~5m
  * Preprocessing 1.3 million images (resizing and SIFT feature extraction): ~1 hour

Input formats
=============

Mycloud has builtin support for processing the following file types:

  * LevelDB
  * CSV
  * Text (lines)
  * Zip

Adding support for your own is simple - just write a resource class describing
how to get a reader and writer.  (see resource.py for details).

Why?!?
======

Sometimes you're developing something in Python (because that's what you do), and
you decide you'd like it to be parallelized.  Our current options are multiprocessing
(limiting us to a single machine) and Hadoop streaming (limiting us to strings and
Hadoop's input formats).

Also, because I could.

Credits
=======

MyCloud builds on the phenomonally useful [cloud](http://pypi.python.org/pypi/cloud/)
serialization, [SSH/Paramiko](http://pypi.python.org/pypi/paramiko/1.9.0), and
[LevelDB](http://pypi.python.org/pypi/leveldb) libraries.
