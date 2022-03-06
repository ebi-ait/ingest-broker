# HCA Ingest Broker

[![Build Status](https://travis-ci.org/HumanCellAtlas/ingest-client.svg?branch=master)](https://travis-ci.org/HumanCellAtlas/ingest-broker)
[![Maintainability](https://api.codeclimate.com/v1/badges/c3cb9256f7e92537fa99/maintainability)](https://codeclimate.com/github/HumanCellAtlas/ingest-broker/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/c3cb9256f7e92537fa99/test_coverage)](https://codeclimate.com/github/HumanCellAtlas/ingest-broker/test_coverage)
[![Docker Repository on Quay](https://quay.io/repository/humancellatlas/ingest-broker/status "Docker Repository on Quay")](https://quay.io/repository/humancellatlas/ingest-broker)


Web endpoint for submitting spreadsheets for HCA Ingest and basic admin UI. 
 
To run scripts locally you'll need Python 3.6 and all the dependencies in [requirements.txt](requirements.txt).

## Setup

```
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## Configuration

The broker uses a locally running ingest core at `localhost:8000` by default. Connecting to a different one
Is done by setting the `INGEST_API` environment variable.

## Running with Python 

Start the web application with 

```bash
python broker/broker_app.py
```

## Running with Flask's CLI

You can use the [flask cli](https://flask.palletsprojects.com/en/2.0.x/cli) to start your app.

Set `FLASK_APP` beforehand:

```bash
export FLASK_APP=broker_app
```

then start flask

```bash
flask run
```

You can set this and other app environment variables such as `INGEST_API` in a `.flaskenv` file.
See the [template](.flaskenv.template).
See more in [flask's docs](https://flask.palletsprojects.com/en/2.0.x/cli/#environment-variables-from-dotenv)

## Running With Docker
Alternatively, you can build and run the app with Docker. To run the web application with Docker for build the Docker image with 

```bash
docker build . -t ingest-broker:latest
```

then run the Docker container. You will need to provide the URL to the [Ingestion API](https://github.com/HumanCellAtlas/ingest-core)

```bash
docker run -p 5000:5000 -e INGEST_API=http://localhost:8080 ingest-broker:latest
```

or run against the development Ingest API
```bash
docker run -p 5000:5000 -e INGEST_API=http://api.ingest.dev.data.humancellatlas.org ingest-broker:latest
```

The application will be available at http://localhost:5000

# Docs

see [design docs](doc/)

## Running unit tests

```bash
nosetests test/unit/
```
