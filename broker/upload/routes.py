import json
import traceback
from dataclasses import dataclass
from http import HTTPStatus

from flask import Blueprint, request
from flask import current_app as app

from flask_cors import cross_origin
from hca_ingest.importer.importer import XlsImporter

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
    storage_service = SpreadsheetStorageService(app.SPREADSHEET_STORAGE_DIR)
    importer = XlsImporter(app.ingest_api)
    spreadsheet_upload_svc = SpreadsheetUploadService(app.ingest_api, storage_service, importer)

    token = request.headers.get('Authorization')
    if not token:
        # TODO need proper validation
        return response_json(HTTPStatus.UNAUTHORIZED, UploadResponse("An authentication token must be supplied when uploading a spreadsheet", "unauthorized"))

    request_file = request.files['file']
    params_str = request.form.get('params')

    if not params_str:
        return response_json(HTTPStatus.BAD_REQUEST, UploadResponse(app.SPREADSHEET_UPLOAD_MESSAGE, 'Missing params'))

    params = json.loads(params_str)
    app.logger.info(f'params: {params_str}')

    try:
        app.logger.info('Uploading spreadsheet!')
        submission_resource = spreadsheet_upload_svc.async_upload(request_file, params)
        app.logger.info(f'Created Submission: {submission_resource["_links"]["self"]["href"]}')
    except SpreadsheetUploadError as error:
        return response_json(error.http_code, UploadResponse(error.message, error.details))
    except Exception as error:
        app.logger.error(traceback.format_exc())
        upload_response = UploadResponse(app.SPREADSHEET_UPLOAD_MESSAGE_ERROR, str(error))
        return response_json(HTTPStatus.INTERNAL_SERVER_ERROR, upload_response)
    else:
        return _create_submission_success_response(submission_resource)


def _create_submission_success_response(submission_resource):
    submission_uuid = submission_resource['uuid']['uuid']
    submission_url = submission_resource['_links']['self']['href']
    submission_id = submission_url.rsplit('/', 1)[-1]

    data = UploadResponse(app.SPREADSHEET_UPLOAD_MESSAGE,
                          {
                              'submission_url': submission_url,
                              'submission_uuid': submission_uuid,
                              'submission_id': submission_id
                          })

    return response_json(201, data)
