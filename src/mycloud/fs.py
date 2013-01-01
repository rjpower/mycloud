#!/usr/bin/env python

'''Filesystem access wrappers, including a trivial remote filesystem.

This is used by mycloud to provide a consistent interface to both local and
distributed files (HDFS).  Also includes the implementation of a trivial 
remote filesystem to allow workers to access files via the master.
'''

from mycloud.config import OPTIONS
import logging
import rpc.client
import urllib2

class FS(object):
  @staticmethod
  def open(f, mode='r'):
    logging.info('Opening %s with mode %s', f, mode)
    # TODO(power) - change this into a URL scheme based registry?
    if f.startswith('client://'):
      return ClientFS.open(f[8:], mode)
    elif f.startswith('local://'):
      return LocalFS.open(f[8:], mode)
    elif f.startswith('http://'):
      return urllib2.urlopen(f)
    else:
      return LocalFS.open(f, mode)
    
  @staticmethod
  def move(src, tgt):
    sf = FS.open(src, 'r')
    tf = FS.open(tgt, 'w')
    while 1:
      buf = tf.read(10000)
      if not buf:
        break
      sf.write(buf)
    tf.close()
  

'''Server side handler for ClientFS files.

Simply dispatches requests to the local filesystem.'''
class ClientFSHandler(object):
  def __init__(self):
    self.files = {}
    self.file_id = 0
  
  def open(self, handle, filename, mode):
    fid = self.file_id
    self.file_id += 1
    self.files[fid] = open(filename, mode)
    handle.done(fid)
  
  def _close(self, handle, fid):
    self.files[fid].close()
    handle.done(None)
    
  def seek(self, handle, fid, pos):
    self.files[fid].seek(pos)
    handle.done(None)
    
  def write(self, handle, fid, data):
    self.files[fid].write(data)
    handle.done(None)
    
  def read(self, handle, fid, num_bytes):
    handle.done(self.files[fid].read(num_bytes))
    

class CloudFile(object):
  def __init__(self, server, filename, mode):
    self.server = server
    self.filename = filename
    self.mode = mode
    self.handle = self.server.open(filename, mode).wait()
    self._buffer = ''
  
  def _read(self, num_bytes):
    if num_bytes < 16000: num_bytes = 16000
    self._buffer += self.server.read(self.handle, num_bytes).wait()
    
  def close(self): self.server._close(self.handle).wait()
  def write(self, buf): self.server.write(self.handle, buf).wait()
     
  def read(self, num_bytes):
    if len(self._buffer) < num_bytes:
      self._read(num_bytes - len(self._buffer))
      
    a, self._buffer = self._buffer[:num_bytes], self._buffer[num_bytes:]
    return a
    
  def seek(self, pos): return self.server.seek(self.handle, pos).wait()
  
  def readline(self):
    l = ''
    while 1:
      c = self.read(1)
      l += c
      if c == '\n' or not c: return l
  
  def __iter__(self):
    while 1:
      l = self.readline()
      if l: yield l
      else: break
    
  
class ClientFS(FS):
  @staticmethod
  def open(filename, mode='r'):
    host = OPTIONS.fs_host
    port = OPTIONS.fs_port
    logging.info('host %s, port %s', host, port)
    return CloudFile(rpc.client.RPCClient(host, port), filename, mode)
  
class LocalFS(FS):
  @staticmethod
  def open(f, mode='r'):
    return open(f, mode)
  
