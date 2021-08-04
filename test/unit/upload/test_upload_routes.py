import unittest
from io import BytesIO
from unittest import TestCase
from unittest.mock import ANY, patch, MagicMock

from broker.service.spreadsheet_upload_service import SpreadsheetUploadError
from broker.upload.routes import UploadResponse
from broker_app import app as _app


class UploadSpreadsheetTestCase(TestCase):

    def setUp(self):
        self.project_uuid = "test-project-uuid"
        self.submission_uuid = "test-submission-uuid"
        self.app = _app.test_client()
        _app.testing = True

    @patch('ingest.api.ingestapi.IngestApi')
    @patch('broker.service.spreadsheet_storage.SpreadsheetStorageService')
    @patch('ingest.importer.importer.XlsImporter')
    @patch('broker.service.spreadsheet_upload_service.SpreadsheetUploadService.async_upload')
    def test_upload_route(self,
                                        mock_upload: MagicMock,
                                        mock_importer,
                                        mock_storage_service,
                                        mock_ingest):
        with _app.test_client() as app:
            # setup test data and dependencies
            mock_upload.return_value = {
                "_links": {"self": {"href": "xxx"}},
                "uuid": {"uuid": self.submission_uuid}
            }
            # when
            response = app.post('/api_upload',
                                data={
                                    "projectUuid": self.project_uuid,
                                    "submissionUuid": self.submission_uuid,
                                    'file': (BytesIO(b'my file contents'), 'file.txt')
                                })
            # then
            self._verify_upload_service_call(mock_upload)
            self._verify_upload_output(response)

    @patch('ingest.api.ingestapi.IngestApi')
    @patch('broker.service.spreadsheet_storage.SpreadsheetStorageService')
    @patch('ingest.importer.importer.XlsImporter')
    @patch('broker.service.spreadsheet_upload_service.SpreadsheetUploadService.async_upload')
    def test_upload_update_route(self,
                                        mock_upload: MagicMock,
                                        mock_importer,
                                        mock_storage_service,
                                        mock_ingest):
        with _app.test_client() as app:
            # setup test data and dependencies
            mock_upload.return_value = {
                "_links": {"self": {"href": "xxx"}},
                "uuid": {"uuid": self.submission_uuid}
            }
            # when
            response = app.post('/api_upload_update',
                                data={
                                    "projectUuid": self.project_uuid,
                                    "submissionUuid": self.submission_uuid,
                                    'file': (BytesIO(b'my file contents'), 'file.txt')
                                })
            # then
            self._verify_upload_service_call(mock_upload, is_update=True)
            self._verify_upload_output(response)

    def _verify_upload_output(self, response):
        response_json = response.get_json()
        self.assertIsNotNone(response_json)
        upload_response = UploadResponse(response_json['message'], response_json['details'])
        self.assertEquals(upload_response.details['submission_uuid'], self.submission_uuid)

    def _verify_upload_service_call(self, mock_upload, is_update=False):
        # verify service call
        mock_upload.assert_called_once_with(
            ANY,  # token
            ANY,  # request_file
            is_update,  # is_update
            self.project_uuid,  # project_uuid
            self.submission_uuid  # : MagicMock
        )

    @patch('broker_app.request')
    @patch('broker.service.spreadsheet_upload_service.SpreadsheetUploadService.async_upload')
    @patch('broker_app.IngestApi')
    def test_upload_success(self, mock_ingest, mock_async_upload, mock_request):
        # given
        mock_async_upload.return_value = {'uuid': {'uuid': 'uuid'}, '_links': {'self': {'href': 'url/9'}}}

        # when
        response = self.app.post('/api_upload',
                                 data={
                                     'file': (BytesIO(b'my file contents'), 'file.txt')
                                 },
                                 headers={
                                     'token':'test-token'
                                 })

        # then
        self.assertEqual(response.status_code, 201)
        self.assertRegex(str(response.data), 'url/9')

    @patch('broker_app.request')
    @patch('broker.service.spreadsheet_upload_service.SpreadsheetUploadService.async_upload')
    @patch('broker_app.IngestApi')
    def test_upload_error(self, mock_ingest, mock_async_upload, mock_request):
        # given
        mock_async_upload.side_effect = SpreadsheetUploadError(500, 'message', 'details')
        # when
        response = self.app.post('/api_upload',
                                 data={
                                     'file': (BytesIO(b'my file contents'), 'file.txt')
                                 },
                                 headers={
                                     'token':'test-token'
                                 })
        # then
        self.assertEqual(500, response.status_code)
        self.assertRegex(str(response.data), 'message')

    @patch('broker_app.request')
    @patch('broker_app.IngestApi')
    def test_upload_unauthorized(self, mock_ingest, mock_request):
        # when
        response = self.app.post('/api_upload',
                                 data={
                                     'file': (BytesIO(b'my file contents'), 'file.txt')
                                 },
                                 headers={
                                     'token':'test-token'
                                 })

        # then
        self.assertEqual(401, response.status_code)
        self.assertRegex(str(response.data), 'authentication')

    @patch('broker_app.request')
    @patch('broker.service.spreadsheet_upload_service.SpreadsheetUploadService.async_upload')
    @patch('broker_app.IngestApi')
    def test_upload_update_success(self, mock_ingest, mock_async_upload, mock_request):
        # given
        mock_async_upload.return_value = {'uuid': {'uuid': 'uuid'}, '_links': {'self': {'href': 'url/9'}}}

        # when
        response = self.app.post('/api_upload_update',
                                 data={
                                     'file': (BytesIO(b'my file contents'), 'file.txt')
                                 },
                                 headers={
                                     'token':'test-token'
                                 })

        # then
        self.assertEqual(response.status_code, 201)
        self.assertRegex(str(response.data), 'url/9')

    @patch('broker_app.request')
    @patch('broker.service.spreadsheet_upload_service.SpreadsheetUploadService.async_upload')
    @patch('broker_app.IngestApi')
    def test_upload_update_error(self, mock_ingest, mock_async_upload, mock_request):
        # given
        mock_async_upload.side_effect = SpreadsheetUploadError(500, 'message', 'details')

        # when
        response = self.app.post('/api_upload_update',
                                 data={
                                     'file': (BytesIO(b'my file contents'), 'file.txt')
                                 },
                                 headers={
                                     'token':'test-token'
                                 })

        # then
        self.assertEqual(response.status_code, 500)
        self.assertRegex(str(response.data), 'message')

    @patch('broker_app.request')
    @patch('broker_app.IngestApi')
    def test_upload_update_unauthorized(self, mock_ingest, mock_request):
        # when
        response = self.app.post('/api_upload_update',
                                 data={
                                     'file': (BytesIO(b'my file contents'), 'file.txt')
                                 },
                                 headers={
                                     'token':'test-token'
                                 })

        # then
        self.assertEqual(response.status_code, 401)
        self.assertRegex(str(response.data), 'authentication')


if __name__ == '__main__':
    unittest.main()
