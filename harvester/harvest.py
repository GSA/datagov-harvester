## common classes
# for lack of a better place to put them

from dataclasses import dataclass


@dataclass
class Source:
    """Class for Harvest Sources"""

    url: str
    source_type: str = ""
    records: dict = {}
    source_type: str = ""  # ckan vs external(dcat, waf)
    ckan_query: str = ""


@dataclass(eq=True)
class Record:
    """Class for Harvest Records"""

    identifier: str
    data: dict = {}
    hash: str = ""


# class DCATUSCatalog:
#     def __init__(self) -> None:
#         pass
