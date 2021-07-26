import io
import tempfile
import time

import jsonpickle
from flask import Blueprint, send_file
from flask import current_app as app

from broker.service.spreadsheet_storage.spreadsheet_storage_exceptions import SubmissionSpreadsheetDoesntExist
from broker.service.spreadsheet_storage.spreadsheet_storage_service import SpreadsheetStorageService
from broker.service.summary_service import SummaryService
from broker.submissions.export_to_spreadsheet_service import ExportToSpreadsheetService

submissions_bp = Blueprint(
    'submissions', __name__, url_prefix='/submissions'
)


@submissions_bp.route('/<submission_uuid>/spreadsheet', methods=['GET'])
def export_to_spreadsheet(submission_uuid):
    workbook = ExportToSpreadsheetService(app.ingest_api).export(submission_uuid)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    temp_file = tempfile.NamedTemporaryFile()
    filename = f'{submission_uuid}_{timestamp}.xlsx'
    workbook.save(temp_file.name)
    return send_file(temp_file.name, as_attachment=True, cache_timeout=0, attachment_filename=filename)


@submissions_bp.route('/<submission_uuid>/spreadsheet/original', methods=['GET'])
def get_submission_spreadsheet(submission_uuid):
    try:
        spreadsheet = SpreadsheetStorageService(app.SPREADSHEET_STORAGE_DIR).retrieve_submission_spreadsheet(submission_uuid)
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


@submissions_bp.route('/<submission_uuid>/summary', methods=['GET'])
def submission_summary(submission_uuid):
    submission = app.ingest_api.get_submission_by_uuid(submission_uuid)
    summary = SummaryService(app.ingest_api).summary_for_submission(submission)

    return app.response_class(
        response=jsonpickle.encode(summary, unpicklable=False),
        status=200,
        mimetype='application/json'
    )
