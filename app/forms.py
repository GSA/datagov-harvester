import os
import re

from flask_wtf import FlaskForm
from wtforms import (
    BooleanField,
    SelectField,
    StringField,
    SubmitField,
    TextAreaField,
)
from wtforms.validators import (
    URL,
    DataRequired,
    Length,
    Optional,
    Regexp,
    ValidationError,
)

is_prod = os.getenv("FLASK_ENV") == "production"


def validate_email_list(form, field):
    emails = field.data
    for email in emails:
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email.strip()):
            raise ValidationError("Invalid email address: {}".format(email))


def strip_filter(data):
    return data.strip() if isinstance(data, str) else data


class EmailListField(TextAreaField):
    def process_formdata(self, valuelist):
        if valuelist:
            raw_data = valuelist[0].replace("\r\n", ", ")
            self.data = [email.strip() for email in raw_data.split(",")]
        else:
            self.data = []


class HarvestSourceForm(FlaskForm):
    organization_id = SelectField(
        "Organization", choices=[], validators=[DataRequired()]
    )
    name = StringField(
        "Name",
        validators=[DataRequired()],
        filters=[strip_filter]
    )
    url = StringField(
        "URL",
        validators=[DataRequired(), URL(require_tld=is_prod)],
        filters=[strip_filter]
    )
    notification_emails = EmailListField(
        "Notification_emails", validators=[DataRequired(), validate_email_list]
    )
    frequency = SelectField(
        "Frequency",
        choices=["manual", "daily", "weekly", "biweekly", "monthly"],
        validators=[DataRequired()],
    )
    schema_type = SelectField(
        "Schema Type",
        choices=[
            "dcatus1.1: federal",
            "dcatus1.1: non-federal",
            "iso19115_1",
            "iso19115_2",
        ],
        validators=[DataRequired()],
    )
    source_type = SelectField(
        "Source Type", choices=["document", "waf"], validators=[DataRequired()]
    )
    notification_frequency = SelectField(
        "Notification Frequency",
        choices=[
            "on_error",
            "always",
            "on_error_or_update",
        ],
        validators=[DataRequired()],
    )


class OrganizationForm(FlaskForm):
    name = StringField(
        "Name",
        validators=[DataRequired()],
        filters=[strip_filter]
    )
    slug = StringField(
        "Slug",
        description=(
            "Use lowercase letters, digits, and hyphens. "
            "For example: 'department-of-energy' or 'gsa'."
        ),
        validators=[
            DataRequired(),
            Length(max=100),
            Regexp(
                r"^[a-z0-9-]*$",
                message=(
                    "Slug can only contain lowercase letters, digits, and hyphens."
                ),
            ),
        ],
        filters=[strip_filter],
    )
    logo = StringField(
        "Logo",
        validators=[DataRequired(), URL()],
        filters=[strip_filter]
    )
    description = TextAreaField(
        "Description",
        validators=[Optional()],
        filters=[strip_filter],
    )

    organization_type = SelectField(
        "Organization Type",
        choices=[
            ("", "Select an organization type"),
            ("Federal Government", "Federal Government"),
            ("City Government", "City Government"),
            ("State Government", "State Government"),
            ("County Government", "County Government"),
            ("University", "University"),
            ("Tribal", "Tribal"),
            ("Non-Profit", "Non-Profit"),
        ],
        validators=[Optional()],
        default="",
    )

    def __init__(self, *args, **kwargs):
        self.organization_id = kwargs.pop("organization_id", None)
        self.db_interface = kwargs.pop("db_interface", None)
        super().__init__(*args, **kwargs)

    def validate_slug(self, field):
        from database.interface import HarvesterDBInterface

        db_interface = self.db_interface or HarvesterDBInterface()
        existing = db_interface.get_organization_by_slug(field.data)

        if existing and existing.id != self.organization_id:
            raise ValidationError("Slug must be unique.")


class HarvestTriggerForm(FlaskForm):
    edit = SubmitField("Edit")
    harvest = SubmitField("Harvest")
    clear = SubmitField(
        "Clear",
    )
    delete = SubmitField("Delete")
    force_check = BooleanField("Force Update")


class OrganizationTriggerForm(FlaskForm):
    edit = SubmitField("Edit")
    delete = SubmitField("Delete")
