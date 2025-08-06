FROM python:3.13

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
