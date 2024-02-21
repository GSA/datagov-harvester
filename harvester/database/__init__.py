import os
import json

db_name = os.getenv('db_name')

def get_database_uri():
    if 'VCAP_SERVICES' in os.environ:
        vcap_services = json.loads(os.environ['VCAP_SERVICES'])
        for db in vcap_services.get('aws-rds', []):
            if db.get('instance_name') == db_name:
                db_info = db.get('credentials', {})
                if db_info:
                    return f"postgresql://{db_info['username']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['db_name']}"
    else:
        return os.getenv('DATABASE_URI')

DATABASE_URI = get_database_uri()