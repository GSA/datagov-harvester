# critical exceptions


# irrecoverable/critical exceptions
class ExtractHarvestSourceException(Exception):
    pass


class ExtractCKANSourceException(Exception):
    pass


class CompareException(Exception):
    pass


# non-critical exceptions
class ValidationException(Exception):
    pass


class TranformationException(Exception):
    pass


class DCATUSToCKANException(Exception):
    pass


class SynchronizeException(Exception):
    pass
