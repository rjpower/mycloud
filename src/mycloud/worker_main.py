#!/usr/bin/env python

import argparse
from mycloud import worker

if __name__ == '__main__':
  p = argparse.ArgumentParser()
  p.add_argument('--logger_host', type=str)
  p.add_argument('--logger_port', type=int)

  opts = p.parse_args()
  worker.run_worker(opts.logger_host, opts.logger_port)
