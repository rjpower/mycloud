#!/usr/bin/env python

import mycloud.cluster
import mycloud.connections
import mycloud.mapreduce
import mycloud.merge
import mycloud.resource
import mycloud.util
import logging

logging.basicConfig(format='%(asctime)s %(filename)s:%(funcName)s %(message)s',
                    level=logging.INFO)

resource = mycloud.resource
mapreduce = mycloud.mapreduce

Cluster = mycloud.cluster.Cluster
