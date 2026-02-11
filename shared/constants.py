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

RECORD_STATUS_VALUES = ["error", "success"]

SCHEMA_TYPE_VALUES = [
    "iso19115_1",
    "iso19115_2",
    "dcatus1.1: federal",
    "dcatus1.1: non-federal",
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
    "SCHEMA_TYPE_VALUES",
    "SOURCE_TYPE_VALUES",
]
