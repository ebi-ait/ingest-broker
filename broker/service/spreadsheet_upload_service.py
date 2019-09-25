import logging
import threading

from ingest.api.ingestapi import IngestApi
from ingest.importer.importer import XlsImporter
from werkzeug.utils import secure_filename

from broker.service.spreadsheet_storage.spreadsheet_storage_service import SpreadsheetStorageService

_LOGGER = logging.getLogger(__name__)


class SpreadsheetUploadService:
    def __init__(self, ingest_api: IngestApi, storage_service: SpreadsheetStorageService, importer: XlsImporter):
        self.ingest_api = ingest_api
        self.storage_service = storage_service
        self.importer = importer

    def async_upload(self, token, request_file, is_update, project_uuid=None):
        if token is None:
            raise SpreadsheetUploadError(401, "An authentication token must be supplied when uploading a spreadsheet")

        self.ingest_api.set_token(token)
        submission_resource = self.ingest_api.create_submission(update_submission=is_update)

        filename = secure_filename(request_file.filename)

        submission_uuid = submission_resource["uuid"]["uuid"]
        path = self.storage_service.store(submission_uuid, filename, request_file.read())

        thread = threading.Thread(target=self._upload,
                                  args=(submission_resource, path, project_uuid))
        thread.start()
        return submission_resource

    def _upload(self, submission_resource, path, project_uuid=None):
        _LOGGER.info('Spreadsheet started!')
        submission_url = submission_resource["_links"]["self"]["href"].rsplit("{")[0]
        submission, template_manager = self.importer.import_file(path, submission_url, project_uuid)
        XlsImporter.create_update_spreadsheet(submission, template_manager, path)
        _LOGGER.info('Spreadsheet upload done!')


class SpreadsheetUploadError(Exception):
    def __init__(self, http_code, message, details=None):
        self.http_code = http_code
        self.message = message
        self.details = details
