## common classes
# for lack of a better place to put them

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class Source:
    """Class for Harvest Sources"""

    url: str
    source_type: str = ""
    records: Dict = field(default_factory=lambda: {})
    source_type: str = ""  # ckan vs external(dcat, waf)
    ckan_query: str = ""


@dataclass(eq=True)
class Record:
    """Class for Harvest Records"""

    identifier: str
    data: Dict = field(default_factory=lambda: {})
    raw_hash: str = ""
    operation: str = ""  # TODO maybe None?


@dataclass
class CompareSet:
    create: List = field(default_factory=lambda: [])
    update: List = field(default_factory=lambda: [])
    delete: List = field(default_factory=lambda: [])


# class DCATUSCatalog:
#     def __init__(self) -> None:
#         pass
