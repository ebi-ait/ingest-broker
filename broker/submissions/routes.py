import io
from http import HTTPStatus

import jsonpickle
from flask import Blueprint, send_file, request
from flask import current_app as app
from hca_ingest.utils.date import parse_date_string

from broker.common.util import response_json
from broker.service.spreadsheet_storage import SubmissionSpreadsheetDoesntExist
from broker.service.spreadsheet_storage import SpreadsheetStorageService
from broker.service.summary_service import SummaryService
from broker.submissions.export_to_spreadsheet_service import ExportToSpreadsheetService

submissions_bp = Blueprint(
    'submissions', __name__, url_prefix='/submissions'
)


@submissions_bp.route('/<submission_uuid>/spreadsheet', methods=['POST'])
def generate_spreadsheet(submission_uuid):
    submission = app.ingest_api.get_submission_by_uuid(submission_uuid)
    spreadsheet_job = submission.get('lastSpreadsheetGenerationJob', {}) or {}

    message = 'The spreadsheet is being generated.'
    if job_not_created(spreadsheet_job) or job_finished(spreadsheet_job):
        token = request.headers.get('Authorization')
        if not token:
            return response_json(HTTPStatus.BAD_REQUEST, {'status': HTTPStatus.BAD_REQUEST,
                                                          'message': 'missing Authorization header'})
        # need a dedicated ingest client that can be authenticated
        ingest_api = app.IngestApi()
        ingest_api.set_token(token)
        spreadsheet_export_service = ExportToSpreadsheetService(app=app, ingest_api=ingest_api)
        job_id = spreadsheet_export_service.async_export_and_save(submission_uuid, app.SPREADSHEET_STORAGE_DIR)
        return response_json(HTTPStatus.ACCEPTED, {'message': message, 'job_id': job_id})
    else:
        return response_json(HTTPStatus.ACCEPTED, {'message': message, 'job_id': spreadsheet_job.get('job_id')})


def job_finished(spreadsheet_job):
    return spreadsheet_job.get('createdDate') and spreadsheet_job.get('finishedDate')


def job_not_created(spreadsheet_job):
    return not spreadsheet_job.get('createdDate')


@submissions_bp.route('/<submission_uuid>/spreadsheet', methods=['GET'])
def download_spreadsheet(submission_uuid):
    submission = app.ingest_api.get_submission_by_uuid(submission_uuid)
    spreadsheet_job = submission.get('lastSpreadsheetGenerationJob', {}) or {}
    # Don't switch to the pythonic 'key' in dictionary pattern here since
    # the service starts jobs with finishedDate set to None
    if spreadsheet_job.get('finishedDate'):
        spreadsheet_details = ExportToSpreadsheetService.get_spreadsheet_details(
            app.SPREADSHEET_STORAGE_DIR, submission_uuid)
        return send_file(spreadsheet_details.filepath,
                         as_attachment=True,
                         cache_timeout=0,
                         attachment_filename=spreadsheet_details.filename)
    elif spreadsheet_job.get('createdDate'):
        return response_json(HTTPStatus.ACCEPTED, {'message': 'The spreadsheet is being generated.'})
    else:
        return response_json(HTTPStatus.NOT_FOUND, {'message': 'The spreadsheet should be generated first.'})


@submissions_bp.route('/<submission_uuid>/spreadsheet/original', methods=['GET'])
def get_submission_spreadsheet(submission_uuid):
    try:
        spreadsheet = SpreadsheetStorageService(app.SPREADSHEET_STORAGE_DIR).retrieve_submission_spreadsheet(
            submission_uuid)
        spreadsheet_name = spreadsheet["name"]
        spreadsheet_blob = spreadsheet["blob"]

        return send_file(
            io.BytesIO(spreadsheet_blob),
            mimetype='application/octet-stream',
            as_attachment=True,
            attachment_filename=spreadsheet_name)
    except SubmissionSpreadsheetDoesntExist as e:
        response_msg = getattr(e, 'message', repr(e))
        err_msg = f'{response_msg}. Missing path: {e.missing_path}'
        app.logger.warning(err_msg)
        return app.response_class(
            response={"message": response_msg},
            status=HTTPStatus.NOT_FOUND,
            mimetype='application/json'
        )


@submissions_bp.route('/<submission_uuid>/summary', methods=['GET'])
def submission_summary(submission_uuid):
    submission = app.ingest_api.get_submission_by_uuid(submission_uuid)
    summary = SummaryService(app.ingest_api).summary_for_submission(submission)

    return app.response_class(
        response=jsonpickle.encode(summary, unpicklable=False),
        status=HTTPStatus.OK,
        mimetype='application/json'
    )
