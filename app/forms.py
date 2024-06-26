import re

from flask_wtf import FlaskForm
from wtforms import SelectField, StringField, TextAreaField
from wtforms.validators import URL, DataRequired, ValidationError


def validate_email_list(form, field):
    emails = field.data.split(",")
    for email in emails:
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email.strip()):
            raise ValidationError("Invalid email address: {}".format(email))


class HarvestSourceForm(FlaskForm):
    organization_id = SelectField(
        "Organization", choices=[], validators=[DataRequired()]
    )
    name = StringField("Name", validators=[DataRequired()])
    url = StringField("URL", validators=[DataRequired(), URL()])
    notification_emails = TextAreaField(
        "Notification_emails", validators=[DataRequired(), validate_email_list]
    )
    frequency = SelectField(
        "Frequency",
        choices=["manual", "daily", "weekly", "biweekly", "monthly"],
        validators=[DataRequired()],
    )
    user_requested_frequency = StringField(
        "User_requested_frequency", validators=[DataRequired()]
    )
    schema_type = SelectField(
        "Schema Type", choices=["strict", "other"], validators=[DataRequired()]
    )
    source_type = SelectField(
        "Source Type", choices=["dcatus", "waf"], validators=[DataRequired()]
    )


class OrganizationForm(FlaskForm):
    name = StringField("Name", validators=[DataRequired()])
    logo = StringField("Logo", validators=[DataRequired()])
