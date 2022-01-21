import io
from datetime import datetime

import jsonpickle
from flask import Blueprint, send_file
from flask import current_app as app

from broker.common.util import response_json
from broker.service.spreadsheet_storage import SubmissionSpreadsheetDoesntExist
from broker.service.spreadsheet_storage import SpreadsheetStorageService
from broker.service.summary_service import SummaryService
from broker.submissions.export_to_spreadsheet_service import ExportToSpreadsheetService

submissions_bp = Blueprint(
    'submissions', __name__, url_prefix='/submissions'
)


@submissions_bp.route('/<submission_uuid>/spreadsheet', methods=['POST'])
def export_to_spreadsheet(submission_uuid):
    submission = app.ingest_api.get_submission_by_uuid(submission_uuid)
    spreadsheet_job = submission.get('lastSpreadsheetDownloadJob', {})

    if not spreadsheet_job.get('createdDate') or (
            spreadsheet_job.get('createdDate') and spreadsheet_job.get('finishedDate')):
        spreadsheet_export_service = ExportToSpreadsheetService(app.ingest_api)
        spreadsheet_export_service.async_export(submission_uuid, app.SPREADSHEET_STORAGE_DIR)
        return response_json(202, {})
    else:
        return response_json(400, {'message': 'Generation ongoing!'})


@submissions_bp.route('/<submission_uuid>/spreadsheet', methods=['GET'])
def download_spreadsheet(submission_uuid):
    submission = app.ingest_api.get_submission_by_uuid(submission_uuid)
    spreadsheet_job = submission.get('lastSpreadsheetDownloadJob', {})
    if spreadsheet_job.get('finishedDate'):
        spreadsheet_job.get('createdDate')
        create_date = datetime.strptime(spreadsheet_job.get('createdDate'), "%Y-%m-%dT%H:%M:%S.%fZ")
        timestamp = create_date.strftime("%Y%m%d-%H%M%S")
        directory = f'{app.SPREADSHEET_STORAGE_DIR}/{submission_uuid}'
        filename = f'{submission_uuid}_{timestamp}.xlsx'
        filepath = f'{directory}/downloads/{filename}'
        return send_file(filepath,
                         as_attachment=True,
                         cache_timeout=0,
                         attachment_filename=filename)
    elif spreadsheet_job.get('createdDate'):
        response_json(401, {'message': 'The spreadsheet is not generated yet'})
    else:
        response_json(401, {'message': 'The spreadsheet is being generated'})


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
            status=404,
            mimetype='application/json'
        )


@submissions_bp.route('/<submission_uuid>/summary', methods=['GET'])
def submission_summary(submission_uuid):
    submission = app.ingest_api.get_submission_by_uuid(submission_uuid)
    summary = SummaryService(app.ingest_api).summary_for_submission(submission)

    return app.response_class(
        response=jsonpickle.encode(summary, unpicklable=False),
        status=200,
        mimetype='application/json'
    )
