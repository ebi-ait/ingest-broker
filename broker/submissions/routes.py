import os
import tempfile
import time

import jsonpickle
from flask import Blueprint, send_from_directory, send_file
from flask import current_app as app

from broker.service.summary_service import SummaryService
from broker.submissions.export_to_spreadsheet_service import ExportToSpreadsheetService

submissions_bp = Blueprint(
    'submissions', __name__, url_prefix='/submissions'
)


@submissions_bp.route('/<submission_uuid>/spreadsheet', methods=['GET'])
def export_to_spreadsheet(submission_uuid):
    workbook = ExportToSpreadsheetService(app.ingest_api).export(submission_uuid)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    # TODO use temporary file / directory
    filename = f'{submission_uuid}_{timestamp}.xlsx'
    workbook.save(filename)
    # TODO investigate if send_file is doing some caching
    return send_file(filename, as_attachment=True)


@submissions_bp.route('/<submission_uuid>/summary', methods=['GET'])
def submission_summary(submission_uuid):
    submission = app.ingest_api.get_submission_by_uuid(submission_uuid)
    summary = SummaryService(app.ingest_api).summary_for_submission(submission)

    return app.response_class(
        response=jsonpickle.encode(summary, unpicklable=False),
        status=200,
        mimetype='application/json'
    )
