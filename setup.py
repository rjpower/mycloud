#!/usr/bin/env python

from setuptools import setup
import os.path
README = open(
    os.path.abspath(os.path.dirname(__file__)) + '/README').read()

setup(
    name="mycloud",
    description="Work distribution for small clusters.",
    long_description=README,
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
    version="0.39",
    url="http://github.com/rjpower/mycloud",
    data_files = [('.', ['README'])],
    package_dir={ '' : 'src' },
    packages=[ 'mycloud' ],
    install_requires=[
      'blocked_table',
      'cloud',
      'pycrypto',
      'speedy',
      'ssh',
    ],
)
