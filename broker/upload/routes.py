import traceback
from dataclasses import dataclass

from flask import Blueprint, current_app, request
from flask_cors import cross_origin
from ingest.importer.importer import XlsImporter

from broker.common.util import response_json
from broker.service.spreadsheet_storage import SpreadsheetStorageService
from broker.service.spreadsheet_upload_service import SpreadsheetUploadService, SpreadsheetUploadError

upload_bp = Blueprint(
    'upload', __name__, url_prefix='/'
)


@dataclass
class UploadResponse:
    message: str
    details: object


@upload_bp.route('/api_upload', methods=['POST'])
@cross_origin()
def upload_spreadsheet():
    return _upload_spreadsheet()


@upload_bp.route('/api_upload_update', methods=['POST'])
@cross_origin()
def upload_update_spreadsheet():
    return _upload_spreadsheet(is_update=True)


def _upload_spreadsheet(is_update=False):
    storage_service = SpreadsheetStorageService(current_app.SPREADSHEET_STORAGE_DIR)
    importer = XlsImporter(current_app.ingest_api)
    spreadsheet_upload_svc = SpreadsheetUploadService(current_app.ingest_api, storage_service, importer)

    token = request.headers.get('Authorization')
    request_file = request.files['file']
    project_uuid = request.form.get('projectUuid')
    submission_uuid = request.form.get('submissionUuid')

    try:
        current_app.logger.info('Uploading spreadsheet!')
        submission_resource = spreadsheet_upload_svc.async_upload(token, request_file, is_update, project_uuid, submission_uuid)
        current_app.logger.info(f'Created Submission: {submission_resource["_links"]["self"]["href"]}')
    except SpreadsheetUploadError as error:
        return response_json(error.http_code, UploadResponse(error.message, error.details))
    except Exception as error:
        current_app.logger.error(traceback.format_exc())
        upload_response = UploadResponse(current_app.SPREADSHEET_UPLOAD_MESSAGE_ERROR, str(error))
        return response_json(500, upload_response)
    else:
        return _create_submission_success_response(submission_resource)


def _create_submission_success_response(submission_resource):
    submission_uuid = submission_resource['uuid']['uuid']
    submission_url = submission_resource['_links']['self']['href']
    submission_id = submission_url.rsplit('/', 1)[-1]

    data = UploadResponse(current_app.SPREADSHEET_UPLOAD_MESSAGE,
                           {
                                'submission_url': submission_url,
                                'submission_uuid': submission_uuid,
                                'submission_id': submission_id
                            })

    return response_json(201, data)
