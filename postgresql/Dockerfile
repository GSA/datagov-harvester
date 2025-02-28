FROM postgis/postgis:17-3.5

ADD https://github.com/GSA/datagov-deploy/raw/71936f004be1882a506362670b82c710c64ef796/ansible/roles/software/ec2/ansible/files/locations.sql.gz /docker-entrypoint-initdb.d/locations.sql.gz
RUN chmod 666 /docker-entrypoint-initdb.d/locations.sql.gz