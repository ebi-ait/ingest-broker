import json
import unittest
from http import HTTPStatus
from unittest.mock import patch, Mock, MagicMock

from broker.submissions import ExportToSpreadsheetService
from test.unit.test_broker_app import BrokerAppTest


class RouteTestCase(BrokerAppTest):
    """
    tests the submissions route
    """
    def setUp(self):
        super().setUp()
        self._app.SPREADSHEET_STORAGE_DIR = 'mock_storage_dir'
        submission = {
            'lastSpreadsheetGenerationJob': {
                'finishedDate': '2022-01-27T12:00:58.417Z',
                'createdDate': '2022-01-27T11:57:05.187Z'
            }
        }
        self.mock_submission = Mock(return_value=submission)
        self.mock_ingest.get_submission_by_uuid = self.mock_submission

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

    def test_download_spreadsheet__not_found(self):
        # given
        self.mock_submission.return_value = {}
        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)

    @patch('broker.submissions.routes.send_file')
    def test_download_spreadsheet__accepted(self, mock_send_file):
        # given
        self.mock_submission.return_value = {
            'lastSpreadsheetGenerationJob': {
                'finishedDate': None,
                'createdDate': '2022-01-27T11:57:05.187Z'
            }
        }
        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.ACCEPTED)
        mock_send_file.assert_not_called()

    @patch.object(ExportToSpreadsheetService, 'async_export_and_save', return_value='test-job-id')
    def test_generate_spreadsheet__accepted__finished(self, mock_async_export_and_save):
        """
        tests the happy path for the controller
        """
        # given
        submission_uuid = 'xyz-001'
        with self._app.test_client() as app:
            # when
            response = app.post(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(HTTPStatus.ACCEPTED, response.status_code)
        mock_async_export_and_save.assert_called_once()

    def test_auth_header__passthrough(self):
        # Given
        submission_uuid = 'xyz-001'
        token = 'test_token'
        self.mock_ingest.set_token = MagicMock()

        with self._app.test_client() as app:
            # when
            response = app.post(
                f'/submissions/{submission_uuid}/spreadsheet',
                headers={'Authorization': token}
            )

        # Then
        self.mock_ingest.set_token.assert_called_once_with(token)
        self.assertEqual(HTTPStatus.ACCEPTED, response.status_code)

    @patch.object(ExportToSpreadsheetService, 'async_export_and_save', return_value='test-job-id')
    def test_generate_spreadsheet__accepted__not_created(self, mock_async_export_and_save):
        # given
        self.mock_submission.return_value = {
            'lastSpreadsheetGenerationJob': None
        }
        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.ACCEPTED)

    @patch.object(ExportToSpreadsheetService, 'async_export_and_save', return_value='test-job-id')
    def test_generate_spreadsheet__accepted__already_created(self, mock_async_export_and_save):
        # given
        self.mock_submission.return_value = {
            'lastSpreadsheetGenerationJob': {
                'createdDate': '2022-01-27T11:57:05.187Z',
                'finishedDate': None
            }
        }
        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.ACCEPTED)


if __name__ == '__main__':
    unittest.main()
