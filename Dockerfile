FROM python:3.10

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

ARG DEV 

RUN if [ $DEV ]; \
    then pip install --no-cache-dir -r requirements-dev.txt; \
    fi

EXPOSE 8080

ENV FLASK_APP=run.py

# Run run.py when the container launches
CMD ["flask", "run", "--host=0.0.0.0", "--port=8080"]