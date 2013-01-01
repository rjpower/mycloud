import logging.handlers
import os
import socket
import tempfile

class Options(object):
  __slots__ = ['temp_prefix', 
               'machines', 
               'max_map_buffer_size',
               'max_reduce_buffer_size', 
               'fs_host', 
               'fs_port', 
               'log_host', 
               'log_port']
  
  def __init__(self):
    self.temp_prefix = tempfile.gettempdir()
    self.machines = ['localhost']
    self.max_map_buffer_size = 32 * 1000 * 1000
    self.max_reduce_buffer_size = 128 * 1000 * 1000
    
    self.fs_host = socket.gethostname()
    self.fs_port = -1
    
    self.log_host = socket.gethostname()
    self.log_port = logging.handlers.DEFAULT_TCP_LOGGING_PORT

def load_config():
  '''Try to load the cluster configuration from (~/.config/mycloud).
  
  If unsuccessful, return a default configuration.
  '''
  try:
    config_file = os.path.expanduser('~/.config/mycloud')
    cluster_globals = { 'Options' : Options }
    cluster_locals = {}
    
    execfile(config_file, cluster_globals, cluster_locals)
    return cluster_locals['options'] 
  except:
    logging.warn('Failed to load configuration from ~/.config/mycloud, using defaults.', exc_info=1)
    return Options()
  
OPTIONS = load_config()
