#!/usr/bin/env python

import io
import logging
import os
import sys
import traceback
from http import HTTPStatus

import jsonpickle
from flask import Flask, request, redirect, send_file, jsonify
from flask import json
from flask_cors import CORS, cross_origin
from ingest.api.ingestapi import IngestApi
from ingest.importer.importer import XlsImporter

from broker.service.schema_service import SchemaService
from broker.service.spreadsheet_storage.spreadsheet_storage_exceptions import SubmissionSpreadsheetDoesntExist
from broker.service.spreadsheet_storage.spreadsheet_storage_service import SpreadsheetStorageService
from broker.service.spreadsheet_upload_service import SpreadsheetUploadService, SpreadsheetUploadError
from broker.service.summary_service import SummaryService
from broker.service.spreadsheet_generation.spreadsheet_generator import SpreadsheetGenerator, SpreadsheetSpec
from broker.service.spreadsheet_generation.spreadsheet_job_manager import SpreadsheetJobManager, SpreadsheetSpec, \
    JobStatus

logging.getLogger('ingest').setLevel(logging.INFO)
logging.getLogger('ingest.api.ingestapi').setLevel(logging.INFO)
logging.getLogger('broker.service.spreadsheet_upload_service').setLevel(logging.INFO)

format = ' %(asctime)s  - %(name)s - %(levelname)s in %(filename)s:' \
         '%(lineno)s %(funcName)s(): %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format=format)

logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
app.secret_key = 'cells'
cors = CORS(app, expose_headers=["Content-Disposition"])
app.config['CORS_HEADERS'] = 'Content-Type'

SPREADSHEET_STORAGE_DIR = os.environ.get('SPREADSHEET_STORAGE_DIR')

SPREADSHEET_UPLOAD_MESSAGE = "We’ve got your spreadsheet, and we’re currently importing and validating the data. \
Nothing else for you to do - check back later."

SPREADSHEET_UPLOAD_MESSAGE_ERROR = "We experienced a problem while uploading your spreadsheet"

ingest_api = IngestApi()
spreadsheet_generator = SpreadsheetGenerator(ingest_api)
spreadsheet_job_manager = SpreadsheetJobManager(spreadsheet_generator, SPREADSHEET_STORAGE_DIR)


@app.route('/', methods=['GET'])
def index():
    new_ui_url = os.environ.get('INGEST_UI')
    if new_ui_url:
        return redirect(new_ui_url, code=302)
    return app.response_class(
        response=json.dumps({'message': "Ingest Broker API is running!"}),
        status=200,
        mimetype='application/json'
    )


@app.route('/api_upload', methods=['POST'])
@cross_origin()
def upload_spreadsheet():
    return _upload_spreadsheet()


@app.route('/api_upload_update', methods=['POST'])
@cross_origin()
def upload_update_spreadsheet():
    return _upload_spreadsheet(is_update=True)


@app.route('/submissions/<submission_uuid>/spreadsheet', methods=['GET'])
def get_submission_spreadsheet(submission_uuid):
    try:
        spreadsheet = SpreadsheetStorageService(SPREADSHEET_STORAGE_DIR).retrieve(submission_uuid)
        spreadsheet_name = spreadsheet["name"]
        spreadsheet_blob = spreadsheet["blob"]

        return send_file(
            io.BytesIO(spreadsheet_blob),
            mimetype='application/octet-stream',
            as_attachment=True,
            attachment_filename=spreadsheet_name)
    except SubmissionSpreadsheetDoesntExist as e:
        return app.response_class(
            response={"message": f'No spreadsheet found for submission with uuid {submission_uuid}'},
            status=404,
            mimetype='application/json'
        )


@app.route('/projects/<project_uuid>/summary', methods=['GET'])
def project_summary(project_uuid):
    project = ingest_api.get_project_by_uuid(project_uuid)
    summary = SummaryService().summary_for_project(project)

    return app.response_class(
        response=jsonpickle.encode(summary, unpicklable=False),
        status=200,
        mimetype='application/json'
    )


@app.route('/submissions/<submission_uuid>/summary', methods=['GET'])
def submission_summary(submission_uuid):
    submission = ingest_api.get_submission_by_uuid(submission_uuid)
    summary = SummaryService().summary_for_submission(submission)

    return app.response_class(
        response=jsonpickle.encode(summary, unpicklable=False),
        status=200,
        mimetype='application/json'
    )


@cross_origin()
@app.route('/spreadsheets', methods=['POST'])
def create_spreadsheet():
    request_json = json.loads(request.data)
    filename = request_json["filename"]
    spreadsheet_spec = SpreadsheetSpec.from_dict(request_json["spec"])
    job_spec = spreadsheet_job_manager.create_job(spreadsheet_spec, filename)

    return app.response_class(
        response=jsonpickle.encode({
            "job_id": job_spec.job_id,
            "_links": {
                "download": {
                    "href": f'/spreadsheets/download/{job_spec.job_id}'
                }
            }
        }, unpicklable=False),
        status=202,
        mimetype='application/hal+json'
    )


@cross_origin()
@app.route('/spreadsheets/download/<job_id>', methods=['GET'])
def get_spreadsheet(job_id: str):
    job_spec = spreadsheet_job_manager.load_job_spec(job_id)
    if job_spec.status == JobStatus.STARTED:
        return app.response_class(
            response=jsonpickle.encode({
                "job_id": job_id,
                "_links": {
                    "download": {
                        "href": f'/spreadsheets/download/{job_id}'
                    }
                }
            }, unpicklable=False),
            status=202,
            mimetype='application/hal+json'
        )
    elif job_spec.status == JobStatus.COMPLETE:
        with spreadsheet_job_manager.spreadsheet_for_job(job_id) as spreadsheet_blob:
            return send_file(
                io.BytesIO(spreadsheet_blob.read()),
                mimetype='application/octet-stream',
                as_attachment=True,
                attachment_filename=job_spec.filename
            )
    elif job_spec.status == JobStatus.ERROR:
        return app.response_class(
            response=jsonpickle.encode(dict(message=f'Server error creating spreadsheet with job id {str(job_id)}.'
                                                    f'Please contact the ingest helpdesk.'),
                                       unpicklable=False),
            status=500,
            mimetype='application/json'
        )
    else:
        return app.response_class(
            response=jsonpickle.encode(dict(message=f'Server error retrieving spreadsheet with job id {str(job_id)}'),
                                       unpicklable=False),
            status=500,
            mimetype='application/json'
        )


# http://0.0.0.0:5000/schemas?high_level_entity=type&domain_entity=biomaterial&concrete_entity=donor_organism&latest&json
# http://0.0.0.0:5000/schemas?url=${schemaUrl}&json&deref`
@app.route('/schemas', methods=['GET'])
def get_schemas():
    args = request.args

    # params
    url = args.get('url')
    high_level_entity = args.get('high_level_entity')
    domain_entity = args.get('domain_entity')
    concrete_entity = args.get('concrete_entity')

    # flags
    json_schema = 'json' in args
    deref = 'deref' in args
    latest = 'latest' in args

    schema_service = SchemaService()

    if not url and latest:
        result = ingest_api.get_schemas(
            latest_only=latest,
            high_level_entity=high_level_entity,
            domain_entity=domain_entity,
            concrete_entity=concrete_entity
        )

        latest_schema = result[0] if len(result) > 0 else None

        if not json_schema:
            return response_json(HTTPStatus.OK, latest_schema)

        url = latest_schema["_links"]["json-schema"]["href"]

    if json_schema and url and deref:
        data = schema_service.get_dereferenced_schema(url)
        return response_json(HTTPStatus.OK, data)

    if json_schema and url and not deref:
        data = schema_service.get_json_schema(url)
        return response_json(HTTPStatus.OK, data)

    return response_json(HTTPStatus.NOT_FOUND, None)


def _upload_spreadsheet(is_update=False):
    storage_service = SpreadsheetStorageService(SPREADSHEET_STORAGE_DIR)
    importer = XlsImporter(ingest_api)
    spreadsheet_upload_svc = SpreadsheetUploadService(ingest_api, storage_service, importer)

    token = request.headers.get('Authorization')
    request_file = request.files['file']
    project_uuid = request.form.get('projectUuid')

    try:
        logger.info('Uploading spreadsheet!')
        submission_resource = spreadsheet_upload_svc.async_upload(token, request_file, is_update, project_uuid)
        logger.info(f'Created Submission: {submission_resource["_links"]["self"]["href"]}')
    except SpreadsheetUploadError as error:
        return response_json(error.http_code, {
            "message": error.message,
            "details": error.details,
        })

    except Exception as error:
        logger.error(traceback.format_exc())
        return response_json(500, {
            "message": SPREADSHEET_UPLOAD_MESSAGE_ERROR,
            "details": str(error),
        })
    else:
        return _create_submission_success_response(submission_resource)


def _create_submission_success_response(submission_resource):
    submission_uuid = submission_resource['uuid']['uuid']
    submission_url = submission_resource['_links']['self']['href']
    submission_id = submission_url.rsplit('/', 1)[-1]

    data = {
        'message': SPREADSHEET_UPLOAD_MESSAGE,
        'details': {
            'submission_url': submission_url,
            'submission_uuid': submission_uuid,
            'submission_id': submission_id
        }
    }

    return response_json(201, data)


def response_json(status_code, data):
    response = app.response_class(
        response=json.dumps(data),
        status=status_code,
        mimetype='application/json'
    )
    return response


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    app.run(host='0.0.0.0', port=5000)
