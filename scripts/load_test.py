
import os
import sys

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from harvester.harvest import HarvestSource

title = os.environ['SRC_TITLE']
url = os.environ['SRC_URL']
owner_org = os.environ['SRC_OWNER_ORG']
source_type = os.environ['SRC_SOURCE_TYPE']

print(title)
print(url)
print(owner_org)
print(source_type)

harvest_source = HarvestSource(
    title,
    url,
    owner_org,
    source_type
)

harvest_source.get_record_changes()
harvest_source.synchronize_records()
harvest_source.report()
