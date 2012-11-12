#!/usr/bin/env python

import threading
import time

def spawn(f, *args, **kw):
  t = threading.Thread(target=f, args=args, kwargs=kw)
  t.setDaemon(kw.get('daemon', True))
  t.start()
  return t

def sleep(timeout):
  time.sleep(timeout)
