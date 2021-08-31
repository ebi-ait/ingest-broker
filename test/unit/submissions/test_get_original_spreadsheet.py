import io
import unittest
from unittest import TestCase
from unittest.mock import patch, MagicMock

from broker.service.spreadsheet_storage import SubmissionSpreadsheetDoesntExist
from broker_app import app as _app


class GetOriginalSpreadsheetTestCase(TestCase):

    def setUp(self):
        _app.testing = True

    @patch('ingest.api.ingestapi.IngestApi')
    @patch('broker.service.spreadsheet_storage.SpreadsheetStorageService.retrieve_submission_spreadsheet')
    def test_original_spreadsheet_route(self, mock_export, mock_ingest):
        # given
        submission_id = 'test-uuid'
        mock_export.return_value = {"name": f'{submission_id}.xlsx', "blob": b'xxx'}

        with _app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_id}/spreadsheet/original')

            # then
            mock_export.assert_called_with(submission_id)
            content_disp = response.headers.get('Content-Disposition')
            self.assertRegex(content_disp, f'filename={submission_id}\\.xlsx')
            self.assertEqual(200, response.status_code)\


    @patch('ingest.api.ingestapi.IngestApi')
    @patch('broker.service.spreadsheet_storage.SpreadsheetStorageService.retrieve_submission_spreadsheet')
    def test_submission_not_found(self, mock_export, mock_ingest):
        # given
        submission_id = 'test-uuid'
        mock_export.side_effect = SubmissionSpreadsheetDoesntExist(submission_id, 'test/path')

        with _app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_id}/spreadsheet/original')

            # then
            self.assertEqual(404, response.status_code)


if __name__ == '__main__':
    unittest.main()
