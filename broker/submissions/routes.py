import tempfile
import time

import jsonpickle
from flask import Blueprint, send_file
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
    temp_file = tempfile.NamedTemporaryFile()
    filename = f'{submission_uuid}_{timestamp}.xlsx'
    workbook.save(temp_file.name)
    return send_file(temp_file.name, as_attachment=True, cache_timeout=0, attachment_filename=filename)


@submissions_bp.route('/<submission_uuid>/summary', methods=['GET'])
def submission_summary(submission_uuid):
    submission = app.ingest_api.get_submission_by_uuid(submission_uuid)
    summary = SummaryService(app.ingest_api).summary_for_submission(submission)

    return app.response_class(
        response=jsonpickle.encode(summary, unpicklable=False),
        status=200,
        mimetype='application/json'
    )
