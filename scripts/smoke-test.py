#!/usr/bin/env python

import os
import sys

from harvester import HarvestSource, Record

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))


harvest_source = HarvestSource("a", "a", "a", "a")

record = Record("a", "a")

print(harvest_source)

print(record)
