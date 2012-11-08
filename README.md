MyCloud
===================

Leverage small clusters of machines to increase your productivity.

mycloud requires no prior setup; if you can SSH to your machines, then
it will work out of the box.  mycloud currently exports a simple 
mapreduce API with several common input formats; adding support for
your own is easy as well.

Usage
=====

Starting your cluster:
  
    # list each machine and the number of cores to use
    cluster = mycloud.Cluster(['machine1', 'machine2'])
     
    # or specify defaults in ~/.config/mycloud
    cluster = mycloud.Cluster()
    
Invoke a function over a list of inputs
  
    result = cluster.map(my_expensive_function, range(1000))

Use the MapReduce interface to easily handle processing of larger datasets
  
    from mycloud.resource import CSV  
    input_desc = [CSV('/path/to/my_input_%d.csv') % i for i in range(100)]
    output_desc = [CSV('/path/to/my_output_file.csv')]
   
    def map_identity(k, v, output):
      output(k, int(v[0]))
  
    def reduce_sum(k, values, output):
      output(k, sum(values))
  
    mr = mycloud.mapreduce.MapReduce(cluster, map_identity, reduce_sum,
                                     input_desc, output_desc)
  
    result = mr.run()
  
    for k, v in result[0].reader():
      print k, v

