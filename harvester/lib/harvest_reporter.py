import math


class HarvestReporter:
    def __init__(self):
        self.total = 0
        self.added = 0
        self.updated = 0
        self.deleted = 0
        self.ignored = 0
        self.errored = 0
        self.validated = 0
        self.action_map = {
            "create": "added",
            "update": "updated",
            "delete": "deleted",
            None: "ignored",
            "errored": "errored",
            "validated": "validated",
        }

    @property
    def percent_complete(self):
        return math.floor(self.processed_count / self.total) * 100

    @property
    def processed_count(self):
        return self.added + self.updated + self.deleted + self.ignored + self.errored

    def __getitem__(self, item):
        return getattr(self, item)

    def update(self, action):
        if not hasattr(self, self.action_map[action]):
            raise Exception(f"{self.action_map[action]} not a supported action")
        setattr(self, self.action_map[action], self[self.action_map[action]] + 1)

    def report(self):
        return {
            "records_total": self.total,
            "records_added": self.added,
            "records_updated": self.updated,
            "records_deleted": self.deleted,
            "records_ignored": self.ignored,
            "records_errored": self.errored,
            "records_validated": self.validated,
        }
