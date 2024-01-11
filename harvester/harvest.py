## common classes
# for lack of a better place to put them

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class Source:
    """Class for Harvest Sources"""

    url: str = ""
    extract_type = ""  # dcat vs waf
    records: Dict = field(default_factory=lambda: {})
    source_type: str = ""  # ckan vs external
    ckan_query: str = ""


@dataclass
class Record:
    """Class for Harvest Records"""

    identifier: str
    raw_metadata: str = ""
    dcatus_metadata: Dict = field(default_factory=lambda: {})
    raw_hash: str = ""
    operation: str = ""  # TODO maybe None?
