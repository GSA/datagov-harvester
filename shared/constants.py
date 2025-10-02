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

__all__ = [
    "ORGANIZATION_TYPE_VALUES",
    "ORGANIZATION_TYPE_SELECT_CHOICES",
]
