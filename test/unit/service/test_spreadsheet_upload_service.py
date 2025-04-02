from unittest import TestCase

from hca_ingest.api.ingestapi import IngestApi
from mock import Mock, MagicMock, patch

from broker.service.spreadsheet_upload_service import SpreadsheetUploadService


class SpreadsheetUploadServiceTest(TestCase):

    def setUp(self) -> None:


        # Create a mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {"uuid":{"uuid":"test_uuid"}}
        mock_response.status_code = 200

        # Create an instance of IngestApi and manually set get to return the mock response
        self.ingest_api = Mock('ingest_api', spec_set=IngestApi)
        self.ingest_api.get = MagicMock(return_value=mock_response)

        self.storage_service = Mock('storage_service')
        self.storage_service.store = Mock(return_value='path')

        self.mock_template_mgr = Mock('template_mgr')
        self.importer = MagicMock('importer')
        self.mock_submission = Mock('submission', return_value={"uuid":{"uuid":"test_uuid"}})
        self.importer.import_file = Mock(return_value=(self.mock_submission, self.mock_template_mgr))
        self.importer.update_spreadsheet_with_uuids = Mock()
        self.spreadsheet_upload_service = spreadsheet_upload_service = SpreadsheetUploadService(self.ingest_api, self.storage_service, self.importer)

    def test_upload_success(self):
        # when
        self.spreadsheet_upload_service.upload('url', 'path')

        # then
        self.importer.import_file.assert_called_with('path', 'url', project_uuid=None, update_project=False)
        self.importer.update_spreadsheet_with_uuids.assert_called_with(self.mock_submission, self.mock_template_mgr, 'path')

    def test_upload_update_success(self):
        # when
        self.spreadsheet_upload_service.upload_updates('url', 'path')

        # then
        self.importer.import_file.assert_called_with('path', 'url', is_update=True)
        self.importer.update_spreadsheet_with_uuids.assert_not_called()
