## common classes
# for lack of a better place to put them


class HarvestSource:
    def __init__(self, url, source_type) -> None:
        self.url = url
        self.source_type = source_type


class HarvestRecord:
    def __init__(self, source) -> None:
        self.harvest_source = source


class DCATUSCatalog:
    def __init__(self) -> None:
        pass
