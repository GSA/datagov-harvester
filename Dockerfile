FROM python:3.14-alpine3.23

# Pull the latest patched packages from the base distro.
RUN apk upgrade --no-cache

WORKDIR /app

RUN pip install poetry && poetry config virtualenvs.create false

# Dependencies first so this layer stays cached until the lockfile changes.
COPY pyproject.toml poetry.lock README.md /app/
RUN poetry install --without=dev --no-root

ARG DEV

RUN if [ $DEV ]; \
    then poetry install --with=dev --no-root; \
    fi

COPY . /app

EXPOSE 8080

ENV FLASK_APP=run.py

# Run run.py when the container launches
CMD ["/bin/sh", "-c", "flask db upgrade && flask run --host=0.0.0.0 --port=8080"]
