[![Build Status](https://travis-ci.org/HumanCellAtlas/ingest-client.svg?branch=master)](https://travis-ci.org/HumanCellAtlas/ingest-broker)
[![Maintainability](https://api.codeclimate.com/v1/badges/c3cb9256f7e92537fa99/maintainability)](https://codeclimate.com/github/HumanCellAtlas/ingest-broker/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/c3cb9256f7e92537fa99/test_coverage)](https://codeclimate.com/github/HumanCellAtlas/ingest-broker/test_coverage)
[![Docker Repository on Quay](https://quay.io/repository/humancellatlas/ingest-broker/status "Docker Repository on Quay")](https://quay.io/repository/humancellatlas/ingest-broker)

# HCA Ingest Broker

Web endpoint for submitting spreadsheets for HCA Ingest and basic admin UI. 

## Web Application 

### Requirements

Requirements for this project are listed in 2 files: `requirements.txt` and `requirements-dev.txt`.
The `requirements-dev.txt` file contains dependencies specific for development

The requirement files (`requirements.txt`, `requirements-dev.txt`) are generated using `pip-compile` from [pip-tools](https://github.com/jazzband/pip-tools) 
```bash
pip-compile requirements.in
pip-compile requirements-dev.in
```
The direct dependencies are listed in `requirements.in`, `requirements-dev.in` input files.

#### Install dependencies

* by using `pip-sync` from `pip-tools`
```bash
pip-sync requirements.txt requirements-dev.txt
```
* or by just using `pip install` 
```bash
    pip install -r requirements.txt
    pip install -r requirements-dev.txt
```

#### Upgrade dependencies

To update all packages, periodically re-run `pip-compile --upgrade`

To update a specific package to the latest or a specific version use the `--upgrade-package` or `-P` flag:

```bash
pip-compile --upgrade-package requests
```

See more options in the pip-compile [documentation](https://github.com/jazzband/pip-tools#updating-requirements) .

### Authenticated Access to Ingest API
The broker needs authenticated access to API. It therefore needs to acquire the token from the service account
similarly to the system tests and graph validation.

TODO: add instructions or link
```
mkdir _local
```
* The GCP credentials are stored in AWS Secrets Manager; To download GCP credentials and save it into a file, the AWS CLI can be used:

```bash
read -p "enter environment [dev,prod]" DEPLOYMENT_ENV
mkdir -p ~/.secrets
chmod 700 ~/.secrets
aws secretsmanager get-secret-value \
  --profile embl-ebi \
  --region us-east-1 \
  --secret-id ingest/${DEPLOYMENT_ENV}/gcp-credentials.json | jq -r .SecretString > ~/.secrets/gcp-credentials-${DEPLOYMENT_ENV}.json
# replace /Users with the home directory location in your env
export GOOGLE_APPLICATION_CREDENTIALS=/Users/$USER/.secrets/gcp-credentials-${DEPLOYMENT_ENV}.json
export INGEST_API_JWT_AUDIENCE=https://dev.data.humancellatlas.org/
```

This behaviour was introduced as part of the Managed Access effort. See ebi-ait/dcp-ingest-central#967

### Running with Python 

Start the web application with 

```bash
python broker/broker_app.py
```

### Running with Flask's CLI

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

### Running With Docker
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
docker run -p 5000:5000 -e INGEST_API=https://api.ingest.dev.archive.data.humancellatlas.org ingest-broker:latest
```

The application will be available at http://localhost:5000

## Tests
### Running all tests
Will send requests to ingest core on dev
```bash
pytest
```

### Running unit tests
Isolated, no external communication required
```bash
pytest test/unit/
```

## Docs

see [design docs](doc/)
