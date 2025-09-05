FROM python:3.12-slim

# Update PAM packages to fix CVE-2025-6020 SNYK-DEBIAN12-PAM-10378969
RUN apt-get install -y --only-upgrade libpam-modules libpam0g libpam-runtime

# increase security by removing unnecessary packages
RUN apt-get upgrade -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

RUN pip install poetry

# poetry try use virtualenv if .venv is present
RUN poetry config virtualenvs.create false
RUN rm -rf /app/.venv 

RUN poetry install --without=dev

ARG DEV

RUN if [ $DEV ]; \
    then poetry install --with=dev; \
    fi

EXPOSE 8080

ENV FLASK_APP=run.py

# Run run.py when the container launches
CMD ["/bin/bash", "-c", "flask db upgrade && flask run --host=0.0.0.0 --port=8080"]
