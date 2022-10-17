#!/usr/bin/env python

import io
import json
import logging.config
import os
from http import HTTPStatus

import jsonpickle
from flask import Flask, request, redirect, send_file
from flask import json
from flask_cors import CORS, cross_origin
from hca_ingest.api.ingestapi import IngestApi

from broker.import_geo.routes import import_geo_bp
from broker.schemas.routes import schemas_bp
from broker.service.spreadsheet_generation.spreadsheet_generator import SpreadsheetGenerator
from broker.service.spreadsheet_generation.spreadsheet_job_manager import \
    SpreadsheetJobManager, \
    SpreadsheetSpec, \
    JobStatus
from broker.service.summary_service import SummaryService
from broker.submissions import submissions_bp
from broker.upload import upload_bp

script_dir = os.path.dirname(os.path.realpath(__file__))
with open(f'{script_dir}/logging-config.json', 'rt') as config_file:
    config = json.load(config_file)
    logging.config.dictConfig(config)


def add_routes(app):
    @app.route('/', methods=['GET'])
    def index():
        new_ui_url = os.environ.get('INGEST_UI')
        if new_ui_url:
            return redirect(new_ui_url, code=302)
        return app.response_class(
            response=json.dumps({'message': "Ingest Broker API is running!"}),
            status=HTTPStatus.OK,
            mimetype='application/json'
        )

    @app.route('/projects/<project_uuid>/summary', methods=['GET'])
    def project_summary(project_uuid):
        project = app.ingest_api.get_project_by_uuid(project_uuid)
        summary = SummaryService(app.ingest_api).summary_for_project(project)

        return app.response_class(
            response=jsonpickle.encode(summary, unpicklable=False),
            status=HTTPStatus.OK,
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
                status=HTTPStatus.ACCEPTED,
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
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
                mimetype='application/json'
            )
        else:
            return app.response_class(
                response=jsonpickle.encode(
                    dict(message=f'Server error retrieving spreadsheet with job id {str(job_id)}'),
                    unpicklable=False),
                status=500,
                mimetype='application/json'
            )


def create_app():
    app = Flask(__name__, static_folder='static')
    app.SPREADSHEET_STORAGE_DIR = os.environ.get('SPREADSHEET_STORAGE_DIR')
    app.SPREADSHEET_UPLOAD_MESSAGE = "We’ve got your spreadsheet, and we’re currently " \
                                     "importing and validating the data. " \
                                     "Nothing else for you to do - check back later."

    app.SPREADSHEET_UPLOAD_MESSAGE_ERROR = "We experienced a problem while uploading your spreadsheet"
    app.secret_key = 'cells'

    CORS(app, expose_headers=["Content-Disposition"])
    app.config['CORS_HEADERS'] = 'Content-Type'

    app.config['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
    app.config['AWS_ACCESS_KEY_SECRET'] = os.getenv('AWS_ACCESS_KEY_SECRET')

    app.ingest_api = IngestApi()
    app.IngestApi = IngestApi
    spreadsheet_generator = SpreadsheetGenerator(app.ingest_api)
    app.spreadsheet_job_manager = SpreadsheetJobManager(spreadsheet_generator, app.SPREADSHEET_STORAGE_DIR)

    app.register_blueprint(upload_bp)
    app.register_blueprint(submissions_bp)
    app.register_blueprint(import_geo_bp)
    app.register_blueprint(schemas_bp)

    add_routes(app)

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=5000)
