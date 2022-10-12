import unittest
from unittest import TestCase
from unittest.mock import patch, Mock

from hca_ingest.api.ingestapi import IngestApi

from broker.service.spreadsheet_storage import SubmissionSpreadsheetDoesntExist
from broker_app import create_app


class GetOriginalSpreadsheetTestCase(TestCase):

    @patch('broker_app.IngestApi')
    @patch('broker_app.SchemaService')
    @patch('broker_app.SpreadsheetGenerator')
    def setUp(self, xls_generator, schema_service, mock_ingest_api_constructor):
        self.mock_ingest = Mock(spec=IngestApi)
        mock_ingest_api_constructor.return_value = self.mock_ingest
        self._app = create_app()
        self._app.config["TESTING"] = True
        self._app.testing = True

    @patch('broker.service.spreadsheet_storage.SpreadsheetStorageService.retrieve_submission_spreadsheet')
    def test_original_spreadsheet_route(self, mock_export):
        # given
        submission_id = 'test-uuid'
        mock_export.return_value = {"name": f'{submission_id}.xlsx', "blob": b'xxx'}

        with self._app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_id}/spreadsheet/original')

            # then
            mock_export.assert_called_with(submission_id)
            content_disp = response.headers.get('Content-Disposition')
            self.assertRegex(content_disp, f'filename={submission_id}\\.xlsx')
            self.assertEqual(200, response.status_code)\


    @patch('broker.service.spreadsheet_storage.SpreadsheetStorageService.retrieve_submission_spreadsheet')
    def test_submission_not_found(self, mock_export):
        # given
        submission_id = 'test-uuid'
        mock_export.side_effect = SubmissionSpreadsheetDoesntExist(submission_id, 'test/path')

        with self._app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_id}/spreadsheet/original')

            # then
            self.assertEqual(404, response.status_code)


if __name__ == '__main__':
    unittest.main()
