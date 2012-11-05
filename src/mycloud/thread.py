#!/usr/bin/env python

import threading
import time

class HelperThread(threading.Thread):
  def __init__(self, f, args):
    self.f = f
    self.args = args
    self.result = None
    threading.Thread.__init__(self)

  def run(self):
    self.result = self.f(*self.args)

  def wait(self):
    self.join()
    return self.result

def spawn(f, *args, **kw):
  t = HelperThread(f, args)
  t.setDaemon(kw.get('daemon', True))
  t.start()
  return t

def sleep(timeout):
  time.sleep(timeout)
