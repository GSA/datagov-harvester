# NOTE: Keep this file in sync between datagov-harvester and datagov-catalog

ACTION_VALUES = ["create", "update", "delete"]

FREQUENCY_VALUES = [
    "manual",
    "daily",
    "weekly",
    "biweekly",
    "monthly",
]

JOB_STATUS_VALUES = [
    "in_progress",
    "complete",
    "new",
    "error",
]

NOTIFICATION_FREQUENCY_VALUES = [
    "on_error",
    "always",
    "on_error_or_update",
]

ORGANIZATION_TYPE_VALUES = (
    "Federal Government",
    "City Government",
    "State Government",
    "County Government",
    "University",
    "Tribal",
    "Non-Profit",
)

ORGANIZATION_TYPE_SELECT_CHOICES = [
    ("", "Select an organization type"),
] + [(value, value) for value in ORGANIZATION_TYPE_VALUES]

RECORD_STATUS_VALUES = ["error", "success", "dataset_pending"]

# All four DCAT-US 3.0 record types from GSA/data.gov#6000 are defined up
# front so sibling tickets (data_series, catalog_record) don't need another
# enum migration; only "dataset" and "data_service" are produced as of #6178.
RECORD_TYPE_VALUES = ["dataset", "data_service", "data_series", "catalog_record"]

SEVERITY_VALUES = ["error", "warning"]

SCHEMA_TYPE_VALUES = [
    "iso19115_1",
    "iso19115_2",
    "dcatus1.1: federal",
    "dcatus1.1: non-federal",
    "dcatus3.0",
]

SOURCE_TYPE_VALUES = ["document", "waf", "waf-collection"]

__all__ = [
    "ACTION_VALUES",
    "FREQUENCY_VALUES",
    "JOB_STATUS_VALUES",
    "NOTIFICATION_FREQUENCY_VALUES",
    "ORGANIZATION_TYPE_VALUES",
    "ORGANIZATION_TYPE_SELECT_CHOICES",
    "RECORD_STATUS_VALUES",
    "RECORD_TYPE_VALUES",
    "SEVERITY_VALUES",
    "SCHEMA_TYPE_VALUES",
    "SOURCE_TYPE_VALUES",
]
