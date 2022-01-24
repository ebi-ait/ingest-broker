import unittest
from unittest import TestCase
from unittest.mock import patch, MagicMock
from broker_app import app as _app


class GetSpreadsheetUsingGeoTestCase(TestCase):

    def setUp(self):
        _app.testing = True


    @patch('broker.import_geo.routes.geo_to_hca.create_spreadsheet_using_geo_accession')
    def test_get_spreadsheet_using_invalid_geo(self, mock_create_spreadsheet_using_geo_accession):
        # given
        mock_workbook = MagicMock()
        mock_create_spreadsheet_using_geo_accession.return_value = mock_workbook
        geo_accession = 'NoAccessionHere'

        with _app.test_client() as app:
            # when
            response = app.post(f'import-geo?accession={geo_accession}')

            # then
            mock_create_spreadsheet_using_geo_accession.assert_not_called()
            self.assertEqual(400, response.status_code)

    @patch('broker.import_geo.routes.geo_to_hca.create_spreadsheet_using_geo_accession')
    def test_get_spreadsheet_using_geo(self, mock_create_spreadsheet_using_geo_accession):
        # given
        mock_workbook = MagicMock()
        mock_create_spreadsheet_using_geo_accession.return_value = mock_workbook
        geo_accession = 'GSE001'

        with _app.test_client() as app:
            # when
            response = app.post(f'import-geo?accession={geo_accession}')

            # then
            mock_create_spreadsheet_using_geo_accession.assert_called_with(geo_accession)
            content_disp = response.headers.get('Content-Disposition')
            self.assertRegex(content_disp, r'filename\=hca_metadata_spreadsheet\-GSE001.xlsx')
            self.assertEqual(200, response.status_code)


if __name__ == '__main__':
    unittest.main()
