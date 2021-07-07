from unittest import TestCase

from mock import Mock, MagicMock, patch

from broker.service.spreadsheet_upload_service import SpreadsheetUploadService


class SpreadsheetUploadServiceTest(TestCase):

    def setUp(self) -> None:
        self.ingest_api = Mock('ingest_api')

        self.storage_service = Mock('storage_service')
        self.storage_service.store = Mock(return_value='path')

        self.mock_submission = Mock('submission')
        self.mock_template_mgr = Mock('template_mgr')
        self.importer = MagicMock('importer')
        self.importer.import_file = Mock(return_value=(self.mock_submission, self.mock_template_mgr))
        self.importer.update_spreadsheet_with_uuids = Mock()
        self.spreadsheet_upload_service = spreadsheet_upload_service = SpreadsheetUploadService(self.ingest_api, self.storage_service, self.importer)

    def test_upload_success(self):
        # when
        self.spreadsheet_upload_service.upload('url', 'path')

        # then
        self.importer.import_file.assert_called_with('path', 'url', project_uuid=None)
        self.importer.update_spreadsheet_with_uuids.assert_called_with(self.mock_submission, self.mock_template_mgr, 'path')

    def test_upload_update_success(self):
        # when
        self.spreadsheet_upload_service.upload_updates('url', 'path')

        # then
        self.importer.import_file.assert_called_with('path', 'url', is_update=True)
        self.importer.update_spreadsheet_with_uuids.assert_not_called()