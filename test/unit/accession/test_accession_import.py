import unittest
from http import HTTPStatus
from unittest.mock import patch, MagicMock

from test.unit.test_broker_app import BrokerAppTest


class AccessionImport(BrokerAppTest):
    @patch('broker.import_geo.routes.geo_to_hca.create_spreadsheet_using_accession')
    def test_get_spreadsheet_using_invalid_accession(self, mock_create_spreadsheet_using_accession):
        # given
        mock_workbook = MagicMock()
        mock_create_spreadsheet_using_accession.return_value = mock_workbook
        accession = 'NoAccessionHere'

        with self._app.test_client() as app:
            # when
            response = app.post(f'spreadsheet-from-accession?accession={accession}')

            # then
            mock_create_spreadsheet_using_accession.assert_not_called()
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    @patch('broker.import_geo.routes.geo_to_hca.create_spreadsheet_using_accession')
    def test_get_spreadsheet_using_valid_accession(self, mock_create_spreadsheet_using_accession):
        # given
        mock_workbook = MagicMock()
        mock_create_spreadsheet_using_accession.return_value = mock_workbook
        accession = 'GSE001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'spreadsheet-from-accession?accession={accession}')

            # then
            mock_create_spreadsheet_using_accession.assert_called_with(accession)
            content_disp = response.headers.get('Content-Disposition')
            self.assertRegex(content_disp, r'filename\=hca_metadata_spreadsheet\-GSE001.xlsx')
            self.assertEqual(HTTPStatus.OK, response.status_code)

    @patch('broker.import_geo.routes.XlsImporter.import_project_from_workbook')
    @patch('broker.import_geo.routes.geo_to_hca.create_spreadsheet_using_accession')
    def test_import_project_using_accession__success(self, mock_create_spreadsheet_using_accession, mock_import):
        # given
        mock_import.return_value = ('project-uuid', [])
        mock_workbook = MagicMock()
        mock_create_spreadsheet_using_accession.return_value = mock_workbook
        accession = 'GSE001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'import-accession?accession={accession}')

            # then
            mock_create_spreadsheet_using_accession.assert_called_with(accession)
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual({'project_uuid': 'project-uuid'}, response.json)

    @patch('broker.import_geo.routes.XlsImporter.import_project_from_workbook')
    @patch('broker.import_geo.routes.geo_to_hca.create_spreadsheet_using_accession')
    def test_import_project_using_accession__error(self, mock_create_spreadsheet_using_accession, mock_import):
        # given
        mock_import.return_value = (None, [{'details': 'error-details'}])
        mock_workbook = MagicMock()
        mock_create_spreadsheet_using_accession.return_value = mock_workbook
        accession = 'GSE001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'import-accession?accession={accession}')

            # then
            mock_create_spreadsheet_using_accession.assert_called_with(accession)
            self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
            self.assertTrue(response.json.get('message'), 'The error response object should have message attribute.')
            self.assertRegex(response.json.get('message'), 'There were errors in importing the project: error-details')

    def test_import_project_using_accession__options(self):
        # given
        accession = 'GSE001'

        with self._app.test_client() as app:
            # when
            response = app.options(f'import-accession?accession={accession}')

            # then
            self.assertEqual(HTTPStatus.OK, response.status_code)


if __name__ == '__main__':
    unittest.main()
