import os
import re

from flask_wtf import FlaskForm
from wtforms import BooleanField, SelectField, StringField, SubmitField, TextAreaField
from wtforms.validators import URL, DataRequired, ValidationError

is_prod = os.getenv("FLASK_ENV") == "production"


def validate_email_list(form, field):
    emails = field.data
    for email in emails:
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email.strip()):
            raise ValidationError("Invalid email address: {}".format(email))


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
    name = StringField("Name", validators=[DataRequired()])
    url = StringField("URL", validators=[DataRequired(), URL(require_tld=is_prod)])
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
    name = StringField("Name", validators=[DataRequired()])
    logo = StringField("Logo", validators=[DataRequired(), URL()])


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
