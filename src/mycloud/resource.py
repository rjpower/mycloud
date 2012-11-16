#!/usr/bin/env python

'''Resources used for input and output to mycloud.

Typically these represent files (or parts of files).
'''

from mycloud.fs import FS
import cPickle
import csv
import leveldb
import mycloud
import os
import subprocess
import zipfile

class ResourceException(Exception):
  pass

class Resource(object):
  def __init__(self, filename):
    self.filename = filename

  def __repr__(self):
    return self.__class__.__name__ + ':' + self.filename

  def writer(self):
    return self.__class__.Writer(self.filename)
  
  def reader(self):
    return self.__class__.Reader(self.filename)

class CSV(Resource):
  class Writer(object):
    def __init__(self, f):
      self.file = FS.open(f, 'w')
      self.csvwriter = csv.writer(self.file)

    def __del__(self):
      self.file.close()

    def add(self, k, v):
      self.csvwriter.writerow([k, v])

  class Reader(object):
    def __init__(self, f):
      self.csvreader = csv.reader(FS.open(f))

    def __iter__(self):
      for row in self.csvreader:
        yield row[0], row[1:]

class Lines(Resource):
  class Writer(object):
    def __init__(self, f):
      self.file = FS.open(f, 'w')
    
    def __del__(self):
      self.file.close()
      
    def add(self, k, v):
      self.file.write('%s\t%s\n' % k, v)
      
  class Reader(object):
    def __init__(self, f):
      self.file = FS.open(f, 'r')
    
    def __iter__(self):
      for line in self.file:
        k, v = line.split('\t')
        yield k, v

class LevelDB(Resource):
  class Reader(object):
    def __init__(self, f):
      self.db = leveldb.LevelDB(f, create_if_missing=False)
    
    def __iter__(self):
      for k, v in self.db.RangeIter():
        yield k, cPickle.loads(v)
  
  class Writer(object):
    def __init__(self, f):
      self.final_name = f
      
      self.tf = mycloud.options().temp_prefix + '.leveldb-tmp-%s' % os.path.basename(self.final_name)
      self.db = leveldb.LevelDB(self.tf)
    
    def __del__(self):
      self.db.CompactRange()
      del self.db
      subprocess.check_call(['mv', self.tf, self.final_name])
    
    def add(self, k, v):
      if not isinstance(k, str): k = str(k)
      v = cPickle.dumps(v, -1)
      self.db.Put(k, v)
  

class Zip(Resource):
  class Reader(object):
    def __init__(self, f):
      self.zip = zipfile.ZipFile(f, mode='r')
    
    def __iter__(self):
      for filename in self.zip.namelist():
        data = self.zip.FS.open(filename, 'r').read()
        yield filename, data
  
  class Writer(object):
    def __init__(self, f):
      self.zip = zipfile.ZipFile(f, mode='w')
    
    def __del__(self):
      self.zip.close()
    
    def add(self, k, v):
      lf = self.zip.FS.open(k, 'w')
      lf.write(v)
      lf.close()
  

class Range(Resource):
  class Reader(object):
    def __init__(self, stop, start, step):
      self.start = start
      self.stop = stop
      self.step = step

    def __iter__(self):
      for i in xrange(self.start, self.stop, self.step):
        yield i, i

  def __init__(self, stop, start=0, step=1):
    Resource.__init__(self, 'Range(%d, %d, %d)' % (start, stop, step))
    self.start = start
    self.stop = stop
    self.step = step

  def reader(self):
    return Range.Reader(self.stop, self.start, self.step)

  def writer(self):
    raise ResourceException, 'Range does not support writing.'


class MemoryFile(Resource):
  class Writer(object):
    def __init__(self, data):
      self.data = data

    def add(self, k, v):
      self.data.append((k, v))

  def __repr__(self):
    return 'MemoryFile(data = %s)' % self.data

  def __init__(self):
    self.filename = 'MemoryFile'
    self.data = []

  def reader(self):
    return iter(self.data)

  def writer(self):
    return MemoryFile.Writer(self.data)
