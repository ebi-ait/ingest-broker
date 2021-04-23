from unittest import TestCase, skip
from unittest.mock import patch, Mock

from broker.service.spreadsheet_upload_service import SpreadsheetUploadError
from broker_app import app as _app, setup
import broker_app

class BrokerAppTest(TestCase):
    @patch('broker_app.SpreadsheetGenerator')
    @patch('broker_app.IngestApi')
    def setUp(self, mock_ingest, mock_spreadsheet_generator):
        setup()

    def run(self, result=None):
        with _app.test_request_context():
            with _app.test_client() as client:
                with patch.object(broker_app, "request") as mock_request:
                    self.mock_request = mock_request
                    self.app = client
                    super(BrokerAppTest, self).run(result)

    @patch('broker_app.os.environ')
    @patch('broker_app.IngestApi')
    def test_index(self, mock_ingest, mock_env):
        # given
        mock_env.get.return_value = None
        # when:
        response = self.app.get('/')

        self.assertEqual(response.status_code, 200)

    @patch('broker_app.os.environ')
    @patch('broker_app.IngestApi')
    def test_index_redirect(self, mock_ingest, mock_env):
        # given
        mock_env.get.return_value = 'url'
        # when:
        response = self.app.get('/')
        # then
        self.assertEqual(response.status_code, 302)

    @patch('broker_app.SpreadsheetUploadService.async_upload')
    @patch('broker_app.IngestApi')
    def test_upload_success(self, mock_ingest, mock_async_upload):
        mock_async_upload.return_value = {'uuid': {'uuid': 'uuid'}, '_links': {'self': {'href': 'url/9'}}}
        self.mock_request.files = {'file': 'content'.encode()}
        self.mock_request.form = {}
        mock_headers = Mock('headers')
        mock_headers.get = Mock(return_value='token')
        self.mock_request.headers = mock_headers

        # when
        response = self.app.post('/api_upload')

        # then
        self.assertEqual(response.status_code, 201)
        self.assertRegex(str(response.data), 'url/9')
        mock_async_upload.assert_called_with('token', 'content'.encode(), False, None)

    @patch('broker_app.SpreadsheetUploadService.async_upload')
    @patch('broker_app.IngestApi')
    def test_upload_error(self, mock_ingest, mock_async_upload):
        # given
        mock_async_upload.side_effect = SpreadsheetUploadError(500, 'message', 'details')
        self.mock_request.files = {'file': 'content'.encode()}
        self.mock_request.form = {}
        mock_headers = Mock('headers')
        mock_headers.get = Mock(return_value='token')
        self.mock_request.headers = mock_headers

        # when
        response = self.app.post('/api_upload')

        # then
        self.assertEqual(response.status_code, 500)
        self.assertRegex(str(response.data), 'message')
        mock_async_upload.assert_called_with('token', 'content'.encode(), False, None)

    @patch('broker_app.IngestApi')
    def test_upload_unauthorized(self, mock_ingest):
        # given
        self.mock_request.files = {'file': 'content'.encode()}
        self.mock_request.form = {}
        mock_headers = Mock('headers')
        mock_headers.get = Mock(return_value=None)
        self.mock_request.headers = mock_headers

        # when
        response = self.app.post('/api_upload')

        # then
        self.assertEqual(response.status_code, 401)
        self.assertRegex(str(response.data), 'authentication')

    @patch('broker_app.SpreadsheetUploadService.async_upload')
    @patch('broker_app.IngestApi')
    def test_upload_update_success(self, mock_ingest, mock_async_upload):
        # given
        mock_async_upload.return_value = {'uuid': {'uuid': 'uuid'}, '_links': {'self': {'href': 'url/9'}}}
        self.mock_request.files = {'file': 'content'.encode()}
        self.mock_request.form = {}
        mock_headers = Mock('headers')
        mock_headers.get = Mock(return_value='token')
        self.mock_request.headers = mock_headers

        # when
        response = self.app.post('/api_upload_update')

        # then
        self.assertEqual(response.status_code, 201)
        self.assertRegex(str(response.data), 'url/9')
        mock_async_upload.assert_called_with('token', 'content'.encode(), True, None)

    @patch('broker_app.SpreadsheetUploadService.async_upload')
    @patch('broker_app.IngestApi')
    def test_upload_update_error(self, mock_ingest, mock_async_upload):
        # given
        mock_async_upload.side_effect = SpreadsheetUploadError(500, 'message', 'details')
        self.mock_request.files = {'file': 'content'.encode()}
        self.mock_request.form = {}
        mock_headers = Mock('headers')
        mock_headers.get = Mock(return_value='token')
        self.mock_request.headers = mock_headers

        # when
        response = self.app.post('/api_upload_update')

        # then
        self.assertEqual(response.status_code, 500)
        self.assertRegex(str(response.data), 'message')
        mock_async_upload.assert_called_with('token', 'content'.encode(), True, None)

    @patch('broker_app.IngestApi')
    def test_upload_update_unauthorized(self, mock_ingest):
        # given
        self.mock_request.files = {'file': 'content'.encode()}
        self.mock_request.form = {}
        mock_headers = Mock('headers')
        mock_headers.get = Mock(return_value=None)
        self.mock_request.headers = mock_headers

        # when
        response = self.app.post('/api_upload_update')

        # then
        self.assertEqual(response.status_code, 401)
        self.assertRegex(str(response.data), 'authentication')