#!/usr/bin/env python

import io
import json
import logging
import os
import sys
from http import HTTPStatus

import jsonpickle
from flask import Flask, request, redirect, send_file
from flask import json
from flask_cors import CORS, cross_origin
from ingest.api.ingestapi import IngestApi

from broker.common.util import response_json
from broker.service.schema_service import SchemaService
from broker.service.spreadsheet_generation.spreadsheet_generator import SpreadsheetGenerator
from broker.service.spreadsheet_generation.spreadsheet_job_manager import SpreadsheetJobManager, SpreadsheetSpec, \
    JobStatus
from broker.service.summary_service import SummaryService
from broker.submissions import submissions_bp
from broker.upload import upload_bp
from broker.import_geo.routes import import_geo_bp

logging.getLogger('ingest').setLevel(logging.INFO)
logging.getLogger('ingest.api.ingestapi').setLevel(logging.INFO)
logging.getLogger('broker.service.spreadsheet_upload_service').setLevel(logging.INFO)
logging.getLogger('geo_to_hca').setLevel(logging.INFO)

format = ' %(asctime)s  - %(name)s - %(levelname)s in %(filename)s:' \
         '%(lineno)s %(funcName)s(): %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format=format)


def create_app():
    app = Flask(__name__, static_folder='static')
    app.SPREADSHEET_STORAGE_DIR = os.environ.get('SPREADSHEET_STORAGE_DIR')
    app.SPREADSHEET_UPLOAD_MESSAGE = "We’ve got your spreadsheet, and we’re currently importing and validating the data. \
Nothing else for you to do - check back later."

    app.SPREADSHEET_UPLOAD_MESSAGE_ERROR = "We experienced a problem while uploading your spreadsheet"
    app.secret_key = 'cells'
    cors = CORS(app, expose_headers=["Content-Disposition"])
    app.config['CORS_HEADERS'] = 'Content-Type'

    app.ingest_api = IngestApi()
    spreadsheet_generator = SpreadsheetGenerator(app.ingest_api)
    app.spreadsheet_job_manager = SpreadsheetJobManager(spreadsheet_generator, app.SPREADSHEET_STORAGE_DIR)

    app.register_blueprint(upload_bp)
    app.register_blueprint(submissions_bp)
    app.register_blueprint(import_geo_bp)
    return app


app = create_app()


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


@app.route('/projects/<project_uuid>/summary', methods=['GET'])
def project_summary(project_uuid):
    project = app.ingest_api.get_project_by_uuid(project_uuid)
    summary = SummaryService().summary_for_project(project)

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
    job_spec = app.spreadsheet_job_manager.create_job(spreadsheet_spec, filename)

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
    job_spec = app.spreadsheet_job_manager.load_job_spec(job_id)
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
        with app.spreadsheet_job_manager.spreadsheet_for_job(job_id) as spreadsheet_blob:
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


# TODO Currently, we also have schema endpoints in Ingest Core and technically this can be implemented there
# Those endpoints could also be removed from core and have a separate schema service for retrieving information about
# the metadata schema and integrated with the schema release process

# http://0.0.0.0:5000/schemas?high_level_entity=type&domain_entity=biomaterial&concrete_entity=donor_organism&latest&json
# http://0.0.0.0:5000/schemas?url=${schemaUrl}&json&deref
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
        result = app.ingest_api.get_schemas(
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
