import logging
import os
import threading
import time

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

    def async_upload(self, token, request_file, is_update, project_uuid=None, submission_uuid=None):
        self._assert_token(token)
        submission_resource = self._create_or_get_submission(submission_uuid)

        submission_uuid = submission_resource["uuid"]["uuid"]
        submission_url = submission_resource["_links"]["self"]["href"]
        filename = secure_filename(request_file.filename)

        if is_update:
            path = self._store_spreadsheet_updates(filename, request_file, submission_uuid)
            thread = threading.Thread(target=self.upload_updates, args=(submission_url, path))
        else:
            path = self.storage_service.store_submission_spreadsheet(submission_uuid, filename, request_file.read())
            thread = threading.Thread(target=self.upload, args=(submission_url, path, project_uuid))

        thread.start()

        return submission_resource

    def _store_spreadsheet_updates(self, filename, request_file, submission_uuid):
        submission_directory = self.storage_service.get_submission_dir(submission_uuid)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        filename_with_timestamp = f'{timestamp}_{filename}'
        # TODO This spreadsheet containing the updates is not downloadable anywhere yet
        path = self.storage_service.store_binary_file(submission_directory, filename_with_timestamp,
                                                      request_file.read())
        return path

    def _create_or_get_submission(self, submission_uuid):
        if submission_uuid:
            submission_resource = self.ingest_api.get_submission_by_uuid(submission_uuid)
        else:
            submission_resource = self.ingest_api.create_submission()
        return submission_resource

    def _assert_token(self, token):
        if token is None:
            raise SpreadsheetUploadError(401, "An authentication token must be supplied when uploading a spreadsheet")
        self.ingest_api.set_token(token)

    def upload(self, submission_url, path, project_uuid=None):
        _LOGGER.info('Spreadsheet started!')
        submission, template_manager = self.importer.import_file(path, submission_url, project_uuid=project_uuid)
        self.importer.update_spreadsheet_with_uuids(submission, template_manager, path)
        _LOGGER.info('Spreadsheet upload done!')

    def upload_updates(self, submission_url, path):
        _LOGGER.info('Spreadsheet started!')
        self.importer.import_file(path, submission_url, is_update=True)
        _LOGGER.info('Spreadsheet upload done!')


class SpreadsheetUploadError(Exception):
    def __init__(self, http_code, message, details=None):
        self.http_code = http_code
        self.message = message
        self.details = details
