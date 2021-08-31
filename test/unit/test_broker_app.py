from unittest import TestCase
from unittest.mock import patch

from broker_app import app as _app, create_app

request_ctx = _app.test_request_context()
request_ctx.push()


class BrokerAppTest(TestCase):
    def setUp(self):
        _app.testing = True
        self.app = _app.test_client()

    @patch('broker_app.os.environ')
    @patch('broker_app.IngestApi')
    def test_index(self, mock_ingest, mock_env):
        # given
        mock_env.get.return_value = None
        # when:
        response = self.app.get('/')

        self.assertEqual(response.status_code, 200)

    @patch('broker_app.SpreadsheetJobManager')
    @patch('broker_app.SpreadsheetGenerator')
    @patch('broker_app.IngestApi')
    def test_create_app(self, mock_ingest, mock_spreadsheet_generator, mock_spreadsheet_manager):
        create_app()
        mock_ingest.assert_called_once()
        mock_spreadsheet_generator.assert_called_once_with(mock_ingest())
        mock_spreadsheet_manager.assert_called_once_with(mock_spreadsheet_generator(mock_ingest()), None)

    @patch('broker_app.os.environ')
    @patch('broker_app.IngestApi')
    def test_index_redirect(self, mock_ingest, mock_env):
        app = _app.test_client()
        # given
        mock_env.get.return_value = 'url'
        # when:
        response = self.app.get('/')
        # then
        self.assertEqual(response.status_code, 302)

