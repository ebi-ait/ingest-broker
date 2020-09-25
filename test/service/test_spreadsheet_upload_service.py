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

    def test_upload_success(self):
        # given:
        self.importer.create_update_spreadsheet = Mock()
        spreadsheet_upload_service = SpreadsheetUploadService(self.ingest_api, self.storage_service, self.importer)
        submission_resource = {
            'uuid': {'uuid': 'submission-uuid'},
            '_links': {
                'self': {
                    'href': 'url'
                }
            },
            'isUpdate': False
        }

        # when
        spreadsheet_upload_service._upload(submission_resource, 'path', is_update=False)

        self.importer.create_update_spreadsheet.assert_called_with(self.mock_submission, self.mock_template_mgr, 'path')

    def test_upload_update_success(self):
        # given:
        self.importer.create_update_spreadsheet = Mock()
        spreadsheet_upload_service = SpreadsheetUploadService(self.ingest_api, self.storage_service, self.importer)
        submission_resource = {
            'uuid': {'uuid': 'submission-uuid'},
            '_links': {
                'self': {
                    'href': 'url'
                }
            },
            'isUpdate': True
        }

        # when
        spreadsheet_upload_service._upload(submission_resource, 'path', is_update=True)

        self.importer.create_update_spreadsheet.assert_called_with(self.mock_submission, self.mock_template_mgr, 'path')
