#!/usr/bin/env python

from setuptools import setup

setup(
    name="mycloud",
    description="Work distribution for small clusters.",
    long_description='''
MyCloud
=======

Leverage small clusters of machines to increase your productivity.

mycloud requires no prior setup; if you can SSH to your machines, then
it will work out of the box. mycloud currently exports a simple
mapreduce API with several common input formats; adding support for your
own is easy as well.

Usage
=====

Starting your cluster:

::

    # list each machine and the number of cores to use
    cluster = mycloud.Cluster(['machine1', 'machine2'])

    # or specify defaults in ~/.config/mycloud
    cluster = mycloud.Cluster()

Invoke a function over a list of inputs

::

    result = cluster.map(my_expensive_function, range(1000))

Use the MapReduce interface to easily handle processing of larger
datasets

::

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
    ''',
    classifiers=['Development Status :: 3 - Alpha',
                 'Topic :: Software Development :: Libraries',
                 'Topic :: System :: Clustering',
                 'Topic :: System :: Distributed Computing',
                 'License :: OSI Approved :: BSD License',
                 'Intended Audience :: Developers',
                 'Intended Audience :: System Administrators',
                 'Operating System :: POSIX',
                 'Programming Language :: Python :: 2.5',
                 'Programming Language :: Python :: 2.6',
                 'Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: 3',
                 'Programming Language :: Python :: 3.0',
                 'Programming Language :: Python :: 3.1',
                 'Programming Language :: Python :: 3.2',
                 ],
    author="Russell Power",
    author_email="power@cs.nyu.edu",
    license="BSD",
    version="0.44",
    url="http://github.com/rjpower/mycloud",
    package_dir={ '' : 'src' },
    scripts = ['scripts/cloudp'],
    packages=[ 'mycloud' ],
    install_requires=[
      'blocked_table',
      'cloud',
      'pycrypto',
      'speedy',
      'ssh',
    ],
)
