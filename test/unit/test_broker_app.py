import os
from unittest import TestCase
from unittest.mock import patch, Mock

from hca_ingest.api.ingestapi import IngestApi

from broker.service.spreadsheet_generation.spreadsheet_generator import SpreadsheetGenerator
from broker.service.spreadsheet_generation.spreadsheet_job_manager import SpreadsheetJobManager
from broker_app import create_app


class BrokerAppTest(TestCase):
    @patch('broker_app.IngestApi')
    @patch('broker_app.SpreadsheetGenerator')
    @patch('broker_app.SpreadsheetJobManager')
    def setUp(self, job_constructor, xls_constructor, ingest_constructor):
        self.ingest_constructor = ingest_constructor
        self.xls_constructor = xls_constructor
        self.job_constructor = job_constructor

        self.mock_ingest = Mock(spec=IngestApi)
        self.mock_spreadsheet = Mock(spec=SpreadsheetGenerator)
        self.mock_job_manager = Mock(spec=SpreadsheetJobManager)

        self.ingest_constructor.return_value = self.mock_ingest
        self.xls_constructor.return_value = self.mock_spreadsheet
        self.job_constructor.return_value = self.mock_job_manager

        self._app = create_app()
        self._app.config["TESTING"] = True
        self._app.testing = True

    def test_index(self):
        # Given
        os.environ.clear()
        # when:
        with self._app.test_client() as app:
            response = app.get('/')
        self.assertEqual(200, response.status_code)

    def test_create_app(self):
        self.ingest_constructor.assert_called_once()
        self.xls_constructor.assert_called_once_with(self.mock_ingest)
        self.job_constructor.assert_called_once_with(self.mock_spreadsheet, None)

    def test_index_redirect(self):
        mock_url = 'url'
        os.environ['INGEST_UI'] = mock_url
        # when:
        with self._app.test_client() as app:
            response = app.get('/')
        # then
        self.assertEqual(302, response.status_code)
        self.assertIn(mock_url, response.location)
