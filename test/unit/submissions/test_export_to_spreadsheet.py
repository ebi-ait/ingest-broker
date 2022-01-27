import json
import unittest
from http import HTTPStatus
from unittest import TestCase
from unittest.mock import patch, MagicMock, Mock

from broker_app import create_app


class ExportToSpreadsheetTestCase(TestCase):
    @patch('broker_app.IngestApi')
    @patch('broker_app.SchemaService')
    @patch('broker_app.SpreadsheetGenerator')
    def setUp(self, xls_generator, schema_service, mock_ingest_api_constructor):
        self.mock_ingest = Mock()
        mock_ingest_api_constructor.return_value = self.mock_ingest
        self._app = create_app()
        self._app.config["TESTING"] = True
        self._app.testing = True

        self.mock_submission = {
            'lastSpreadsheetDownloadJob': {
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
            status=200,
            mimetype='application/json'
        )

        submission_id = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_id}/spreadsheet')

            # then
            self.assertEqual(response.status_code, HTTPStatus.OK)
            mock_send_file.assert_called_once()

    @patch('broker.submissions.routes.send_file')
    def test_download_spreadsheet__not_found(self, mock_send_file):
        # given
        self.mock_submission = {}
        self.mock_ingest.get_submission_by_uuid = Mock(return_value=self.mock_submission)

        submission_id = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_id}/spreadsheet')

            # then
            self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
            mock_send_file.assert_not_called()

    @patch('broker.submissions.routes.send_file')
    @patch('broker.submissions.ExportToSpreadsheetService.export')
    def test_download_spreadsheet__accepted(self, mock_export, mock_send_file):
        # given
        self.mock_submission = {
            'lastSpreadsheetDownloadJob': {
                'finishedDate': None,
                'createdDate': '2022-01-27T11:57:05.187Z'
            }
        }
        self.mock_ingest.get_submission_by_uuid = Mock(return_value=self.mock_submission)

        mock_workbook = MagicMock()
        mock_export.return_value = mock_workbook
        submission_id = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_id}/spreadsheet')

            # then
            self.assertEqual(response.status_code, HTTPStatus.ACCEPTED)

    @patch('broker.submissions.ExportToSpreadsheetService.async_export_and_save')
    def test_generate_spreadsheet__accepted__finished(self, mock_export):
        # given
        self.mock_submission = {
            'lastSpreadsheetDownloadJob': {
                'finishedDate': '2022-01-27T12:00:58.417Z',
                'createdDate': '2022-01-27T11:57:05.187Z'
            }
        }
        self.mock_ingest.get_submission_by_uuid = Mock(return_value=self.mock_submission)

        submission_id = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'/submissions/{submission_id}/spreadsheet')

            # then
            self.assertEqual(response.status_code, HTTPStatus.ACCEPTED)
            mock_export.assert_called_once()

    @patch('broker.submissions.ExportToSpreadsheetService.async_export_and_save')
    def test_generate_spreadsheet__accepted__not_created(self, mock_export):
        # given
        self.mock_submission = {
            'lastSpreadsheetDownloadJob': None
        }
        self.mock_ingest.get_submission_by_uuid = Mock(return_value=self.mock_submission)
        submission_id = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'/submissions/{submission_id}/spreadsheet')

            # then
            self.assertEqual(response.status_code, HTTPStatus.ACCEPTED)
            mock_export.assert_called_once()


if __name__ == '__main__':
    unittest.main()
