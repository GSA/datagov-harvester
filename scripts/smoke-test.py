#!/usr/bin/env python

import os
import sys

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from harvester import HarvestSource, Record

harvest_source = HarvestSource('a', 'a', 'a', 'a')

record = Record('a', 'a')

print(harvest_source)

print(record)
