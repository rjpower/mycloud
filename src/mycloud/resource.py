#!/usr/bin/env python

'''Resources used for input and output to mycloud.

Typically these represent files (or parts of files).
'''
import csv
import leveldb
import os
import zipfile

class ResourceException(Exception):
  pass

class Resource(object):
  def __init__(self, filename):
    self.filename = filename

  def __repr__(self):
    return self.__class__.__name__ + ':' + self.filename

  def exists(self):
    return os.path.exists(self.filename)

  def move(self, source, dest):
    os.rename(source, dest)

class CSV(Resource):
  class Writer(object):
    def __init__(self, f):
      self.file = open(f, 'w')
      self.csvwriter = csv.writer(self.file)

    def __del__(self):
      self.file.flush()
      self.file.close()

    def add(self, k, v):
      self.csvwriter.writerow([k, v])

  class Reader(object):
    def __init__(self, f):
      self.csvreader = csv.reader(open(f))

    def __iter__(self):
      for row in self.csvreader:
        yield row[0], row[1:]

  def reader(self):
    return CSV.Reader(self.filename)

  def writer(self):
    return CSV.Writer(self.filename)

class Lines(Resource):
  class Writer(object):
    def __init__(self, f):
      self.file = open(f, 'w')
    
    def __del__(self):
      self.file.flush()
      self.file.close()
      
    def add(self, k, v):
      self.file.write('%s\t%s\n' % k, v)
      
  class Reader(object):
    def __init__(self, f):
      self.file = open(f, 'r')
    
    def __iter__(self):
      for line in self.file:
        k, v = line.split('\t')
        yield k, v

class LevelDB(Resource):
  class Reader(object):
    def __init__(self, f):
      self.db = leveldb.LevelDB(f)
    
    def __iter__(self):
      for k, v in self.db.RangeIter():
        yield k, v
  
  class Writer(object):
    def __init__(self, f):
      self.db = leveldb.LevelDB(f)
    
    def __del__(self):
      del self.db
    
    def add(self, k, v):
      self.db.Put(k, v)
  
  def reader(self): return LevelDB.Reader(self.filename)
  def writer(self): return LevelDB.Writer(self.filename)


class Zip(Resource):
  class Reader(object):
    def __init__(self, f):
      self.zip = zipfile.ZipFile(f, mode='r')
    
    def __iter__(self):
      for filename in self.zip.namelist():
        data = self.zip.open(filename, 'r').read()
        yield filename, data
  
  class Writer(object):
    def __init__(self, f):
      self.zip = zipfile.ZipFile(f, mode='w')
    
    def __del__(self):
      self.zip.close()
    
    def add(self, k, v):
      lf = self.zip.open(k, 'w')
      lf.write(v)
      lf.close()
  
  def reader(self): return Zip.Reader(self.filename)
  def writer(self): return Zip.Writer(self.filename)
  
class Range(Resource):
  class Reader(object):
    def __init__(self, range):
      self.range = range

    def __iter__(self):
      for i in self.range:
        yield i, i

  def __init__(self, range):
    Resource.__init__(self, 'range(%d)' % len(range))
    self.range = range

  def reader(self):
    return Range.Reader(self.range)

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
