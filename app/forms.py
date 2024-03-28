from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, SelectField
from wtforms.validators import DataRequired, URL, ValidationError
import re

def validate_email_list(form, field):
    emails = field.data.split(',')
    for email in emails:
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email.strip()):
            raise ValidationError("Invalid email address: {}".format(email))
        
class HarvestSourceForm(FlaskForm):
    # organization_id = StringField('organization_id', validators=[DataRequired()])
    organization_id = SelectField('Organization', choices=[], validators=[DataRequired()])
    name = StringField('Name', validators=[DataRequired()])
    url = StringField('URL', validators=[DataRequired(), URL()])
    emails = StringField('Notification_emails', validators=[DataRequired(), validate_email_list])
    frequency = SelectField('Frequency', choices=['daily', 'monthly', 'yearly'], validators=[DataRequired()])
    schema_type = StringField('Schema Type', validators=[DataRequired()])
    source_type = StringField('Source Type', validators=[DataRequired()])
    submit = SubmitField('Submit')

class OrganizationForm(FlaskForm):
    name = StringField('Name', validators=[DataRequired()])
    logo = StringField('Logo', validators=[DataRequired()])
    submit = SubmitField('Submit')