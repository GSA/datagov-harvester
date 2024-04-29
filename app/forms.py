import re

from flask_wtf import FlaskForm
from wtforms import SelectField, StringField, SubmitField, TextAreaField
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
    emails = TextAreaField(
        "Notification_emails", validators=[DataRequired(), validate_email_list]
    )
    frequency = SelectField(
        "Frequency",
        choices=["Manual", "Daily", "Weekly", "Biweekly", "Monthly"],
        validators=[DataRequired()],
    )
    user_requested_frequency = StringField(
        "User_requested_frequency", validators=[DataRequired()]
    )
    schema_type = SelectField(
        "Schema Type", choices=["strict", "other"], validators=[DataRequired()]
    )
    source_type = SelectField(
        "Source Type", choices=["Datajson", "WAF"], validators=[DataRequired()]
    )
    submit = SubmitField("Submit")


class OrganizationForm(FlaskForm):
    name = StringField("Name", validators=[DataRequired()])
    logo = StringField("Logo", validators=[DataRequired()])
    submit = SubmitField("Submit")
