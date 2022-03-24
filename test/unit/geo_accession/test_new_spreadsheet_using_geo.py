import unittest
from http import HTTPStatus
from unittest import TestCase
from unittest.mock import patch, MagicMock, Mock
from broker_app import create_app


class GetSpreadsheetUsingGeoTestCase(TestCase):

    @patch('broker_app.IngestApi')
    @patch('broker_app.SchemaService')
    @patch('broker_app.SpreadsheetGenerator')
    def setUp(self, xls_generator, schema_service, mock_ingest_api_constructor):
        self.mock_ingest = Mock()
        mock_ingest_api_constructor.return_value = self.mock_ingest
        self._app = create_app()
        self._app.config["TESTING"] = True
        self._app.testing = True

    @patch('broker.import_geo.routes.geo_to_hca.create_spreadsheet_using_geo_accession')
    def test_get_spreadsheet_using_invalid_geo(self, mock_create_spreadsheet_using_geo_accession):
        # given
        mock_workbook = MagicMock()
        mock_create_spreadsheet_using_geo_accession.return_value = mock_workbook
        geo_accession = 'NoAccessionHere'

        with self._app.test_client() as app:
            # when
            response = app.post(f'import-geo?accession={geo_accession}')

            # then
            mock_create_spreadsheet_using_geo_accession.assert_not_called()
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    @patch('broker.import_geo.routes.geo_to_hca.create_spreadsheet_using_geo_accession')
    def test_get_spreadsheet_using_valid_geo(self, mock_create_spreadsheet_using_geo_accession):
        # given
        mock_workbook = MagicMock()
        mock_create_spreadsheet_using_geo_accession.return_value = mock_workbook
        geo_accession = 'GSE001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'import-geo?accession={geo_accession}')

            # then
            mock_create_spreadsheet_using_geo_accession.assert_called_with(geo_accession)
            content_disp = response.headers.get('Content-Disposition')
            self.assertRegex(content_disp, r'filename\=hca_metadata_spreadsheet\-GSE001.xlsx')
            self.assertEqual(HTTPStatus.OK, response.status_code)

    @patch('broker.import_geo.routes.XlsImporter.import_project_from_workbook')
    @patch('broker.import_geo.routes.geo_to_hca.create_spreadsheet_using_geo_accession')
    def test_import_project_using_geo__success(self, mock_create_spreadsheet_using_geo_accession, mock_import):
        # given
        mock_import.return_value = ('project-uuid', [])
        mock_workbook = MagicMock()
        mock_create_spreadsheet_using_geo_accession.return_value = mock_workbook
        geo_accession = 'GSE001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'import-geo-project?accession={geo_accession}')

            # then
            mock_create_spreadsheet_using_geo_accession.assert_called_with(geo_accession)
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual({'project_uuid': 'project-uuid'}, response.json)

    @patch('broker.import_geo.routes.XlsImporter.import_project_from_workbook')
    @patch('broker.import_geo.routes.geo_to_hca.create_spreadsheet_using_geo_accession')
    def test_import_project_using_geo__error(self, mock_create_spreadsheet_using_geo_accession, mock_import):
        # given
        mock_import.return_value = (None, [{'details': 'error-details'}])
        mock_workbook = MagicMock()
        mock_create_spreadsheet_using_geo_accession.return_value = mock_workbook
        geo_accession = 'GSE001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'import-geo-project?accession={geo_accession}')

            # then
            mock_create_spreadsheet_using_geo_accession.assert_called_with(geo_accession)
            self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
            self.assertTrue(response.json.get('message'), 'The error response object should have message attribute.')
            self.assertRegex(response.json.get('message'), 'There were errors in importing the project: error-details')

    def test_import_project_using_geo__options(self):
        # given
        geo_accession = 'GSE001'

        with self._app.test_client() as app:
            # when
            response = app.options(f'import-geo-project?accession={geo_accession}')

            # then
            self.assertEqual(HTTPStatus.OK, response.status_code)


if __name__ == '__main__':
    unittest.main()