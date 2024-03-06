# Use an official Python runtime as a base image
FROM python:3.11

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Install Poetry
RUN pip install poetry

# Avoid creating virtual environments
RUN poetry config virtualenvs.create false

# Install project dependencies
RUN poetry install

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=8080

# Run Flask
CMD ["poetry", "run", "flask", "run"]
