import unittest
from unittest import TestCase
from unittest.mock import patch
from broker_app import app as _app


class ExportToSpreadsheetTestCase(TestCase):

    def setUp(self):
        _app.testing = True

    @patch('broker.submissions.ExportToSpreadsheetService.export')
    @patch('broker_app.IngestApi')
    def test_export_to_spreadsheet_route(self, mock_ingest, mock_export):
        # given
        mock_export.return_value = 'test export'
        submission_id = 'xyz-001'
        # noting here yet

        with _app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_id}/spreadsheet')

            # then
            mock_export.assert_called_with(submission_id)
            self.assertEqual(200, response.status_code)


if __name__ == '__main__':
    unittest.main()
