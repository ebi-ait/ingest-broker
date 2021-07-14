import unittest
from unittest import TestCase
from unittest.mock import patch, MagicMock
from broker_app import app as _app


class ExportToSpreadsheetTestCase(TestCase):

    def setUp(self):
        _app.testing = True

    @patch('broker.submissions.ExportToSpreadsheetService.export')
    def test_export_to_spreadsheet_route(self, mock_export):
        # given
        mock_workbook = MagicMock()
        mock_export.return_value = mock_workbook
        submission_id = 'xyz-001'

        with _app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_id}/spreadsheet')

            # then
            mock_export.assert_called_with(submission_id)
            content_disp = response.headers.get('Content-Disposition')
            self.assertRegex(content_disp, r'filename\=xyz-001.*.xlsx')
            self.assertEqual(200, response.status_code)


if __name__ == '__main__':
    unittest.main()
