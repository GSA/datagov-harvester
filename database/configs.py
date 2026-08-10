from dataclasses import dataclass


@dataclass(frozen=True)
class PaginationConfig:
    entries_per_page: int
    start_page: int = 1
    max_entries_per_page: int = 100
