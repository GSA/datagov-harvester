FROM python:3.12.12-slim

# Refresh APT indexes and pull the latest patched packages from the base distro.
RUN apt-get update && \
    apt-get upgrade -y && \
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
