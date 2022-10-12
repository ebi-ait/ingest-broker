import json
import unittest
from http import HTTPStatus
from unittest.mock import patch, MagicMock, Mock
from broker.submissions import ExportToSpreadsheetService
from test.unit.test_broker_app import BrokerAppTest


class ExportToSpreadsheetTestCase(BrokerAppTest):
    def setUp(self):
        super().setUp()
        self._app.SPREADSHEET_STORAGE_DIR = 'mock_storage_dir'
        self.mock_submission = {
            'lastSpreadsheetGenerationJob': {
                'finishedDate': '2022-01-27T12:00:58.417Z',
                'createdDate': '2022-01-27T11:57:05.187Z'
            }
        }
        self.mock_ingest.get_submission_by_uuid = Mock(return_value=self.mock_submission)

    @patch('broker.submissions.routes.send_file')
    def test_download_spreadsheet__success(self, mock_send_file):
        # given
        mock_send_file.return_value = self._app.response_class(
            response=json.dumps({}),
            status=HTTPStatus.OK,
            mimetype='application/json'
        )

        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_send_file.assert_called_once()

    @patch('broker.submissions.routes.send_file')
    def test_download_spreadsheet__not_found(self, mock_send_file):
        # given
        self.mock_submission = {}
        self.mock_ingest.get_submission_by_uuid = Mock(return_value=self.mock_submission)

        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
        mock_send_file.assert_not_called()

    @patch('broker.submissions.routes.send_file')
    def test_download_spreadsheet__accepted(self, mock_send_file):
        # given
        self.mock_submission = {
            'lastSpreadsheetGenerationJob': {
                'finishedDate': None,
                'createdDate': '2022-01-27T11:57:05.187Z'
            }
        }
        self.mock_ingest.get_submission_by_uuid = Mock(return_value=self.mock_submission)
        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.ACCEPTED)
        mock_send_file.assert_not_called()

    @patch('broker.submissions.ExportToSpreadsheetService.async_export_and_save',
           ExportToSpreadsheetService.export_and_save)
    def test_generate_spreadsheet__accepted__finished(self):
        # given
        self.mock_submission = {
            'lastSpreadsheetGenerationJob': {
                'createdDate': '2022-01-27T11:57:05.187Z',
                'finishedDate': '2022-01-27T12:00:58.417Z'
            }
        }
        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.ACCEPTED)
        self.mock_ingest.assert_called_once_with(submission_uuid)
        # ToDo: Build correct mocking of calls-to and responses-from ingest

    @patch('broker.submissions.ExportToSpreadsheetService.async_export_and_save',
           ExportToSpreadsheetService.export_and_save)
    def test_generate_spreadsheet__accepted__not_created(self):
        # given
        self.mock_submission = {
            'lastSpreadsheetGenerationJob': None
        }
        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.ACCEPTED)
        # ToDo: Build correct mocking of calls-to and responses-from ingest

    @patch('broker.submissions.ExportToSpreadsheetService.async_export_and_save',
           ExportToSpreadsheetService.export_and_save)
    def test_generate_spreadsheet__accepted__already_created(self):
        # given
        self.mock_submission = {
            'lastSpreadsheetGenerationJob': {
                'createdDate': '2022-01-27T11:57:05.187Z',
                'finishedDate': None
            }
        }
        self.mock_ingest.get_submission_by_uuid = Mock(return_value=self.mock_submission)
        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.ACCEPTED)


if __name__ == '__main__':
    unittest.main()
