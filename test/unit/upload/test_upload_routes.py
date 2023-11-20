import json
import unittest
from email.policy import HTTP
from http import HTTPStatus
from io import BytesIO
from unittest.mock import ANY, patch, MagicMock

from broker.service.spreadsheet_upload_service import SpreadsheetUploadError
from broker.upload.routes import UploadResponse

from test.unit.test_broker_app import BrokerAppTest


class UploadSpreadsheetTestCase(BrokerAppTest):
    def setUp(self):
        super().setUp()
        self.project_uuid = "test-project-uuid"
        self.submission_uuid = "test-submission-uuid"
        request_ctx = self._app.test_request_context()
        request_ctx.push()

    @patch('hca_ingest.importer.importer.XlsImporter')
    @patch('broker.service.spreadsheet_storage.SpreadsheetStorageService')
    @patch('broker.service.spreadsheet_upload_service.SpreadsheetUploadService.async_upload')
    def test_upload_route(self, mock_upload: MagicMock,
                          mock_storage_service,
                          mock_importer):
        # given
        mock_upload.return_value = {
            "_links": {"self": {"href": "xxx"}},
            "uuid": {"uuid": self.submission_uuid}
        }

        params = {
            'isUpdate': False,
            'projectUuid': self.project_uuid,
            'submissionUuid': self.submission_uuid}

        # when
        with self._app.test_client() as app:
            response = app.post(
                '/api_upload',
                data={
                    'params': json.dumps(params),
                    'file': (BytesIO(b'my file contents'), 'file.txt')
                },
                headers={
                    'Authorization': 'test-token'
                }
            )
        # then
        self._verify_upload_service_call(mock_upload, params)
        self._verify_upload_output(response)

    @patch('hca_ingest.importer.importer.XlsImporter')
    @patch('broker.service.spreadsheet_storage.SpreadsheetStorageService')
    @patch('broker.service.spreadsheet_upload_service.SpreadsheetUploadService.async_upload')
    def test_upload_update_route(self, mock_upload: MagicMock, mock_storage_service, mock_importer):
        # given
        mock_upload.return_value = {
            "_links": {"self": {"href": "xxx"}},
            "uuid": {"uuid": self.submission_uuid}
        }
        params = {
            'isUpdate': True,
            'projectUuid': self.project_uuid,
            'submissionUuid': self.submission_uuid
        }
        # when
        with self._app.test_client() as app:
            response = app.post(
                '/api_upload',
                data={
                    'params': json.dumps(params),
                    'file': (BytesIO(b'my file contents'), 'file.txt')
                },
                headers={
                    'Authorization': 'test-token'
                }
            )
        # then
        self._verify_upload_service_call(mock_upload, params)
        self._verify_upload_output(response)

    def _verify_upload_output(self, response):
        response_json = response.get_json()
        self.assertIsNotNone(response_json)
        upload_response = UploadResponse(response_json['message'], response_json['details'])
        self.assertEquals(upload_response.details['submission_uuid'], self.submission_uuid)

    def _verify_upload_service_call(self, mock_upload, params: dict):
        mock_upload.assert_called_once_with(
            ANY,  # request_file
            params
        )

    @patch('broker_app.request')
    @patch('broker.service.spreadsheet_upload_service.SpreadsheetUploadService.async_upload')
    def test_upload_success(self, mock_async_upload, mock_request):
        # given
        mock_async_upload.return_value = {
            'uuid': {'uuid': 'uuid'},
            '_links': {'self': {'href': 'url/9'}}
        }

        # when
        with self._app.test_client() as app:
            response = app.post(
                '/api_upload',
                data={
                    'params': '{}',
                    'file': (BytesIO(b'my file contents'), 'file.txt')
                },
                headers={
                    'Authorization': 'test-token'
                }
            )
        # then
        self.assertEqual(response.status_code, HTTPStatus.CREATED)
        self.assertRegex(str(response.data), 'url/9')

    @patch('broker_app.request')
    @patch('broker.service.spreadsheet_upload_service.SpreadsheetUploadService.async_upload')
    def test_upload_error(self, mock_async_upload, mock_request):
        # given
        mock_async_upload.side_effect = SpreadsheetUploadError(HTTPStatus.INTERNAL_SERVER_ERROR, 'message', 'details')

        # when
        with self._app.test_client() as app:
            response = app.post(
                '/api_upload',
                data={
                    'params': '{}',
                    'file': (BytesIO(b'my file contents'), 'file.txt')
                },
                headers={
                    'Authorization': 'test-token'
                }
            )
        # then
        self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
        self.assertRegex(str(response.data), 'message')

    @patch('broker_app.request')
    def test_upload_unauthorized(self, mock_request):
        # when
        with self._app.test_client() as app:
            response = app.post(
                '/api_upload',
                data={
                    'params': '{}',
                    'file': (BytesIO(b'my file contents'), 'file.txt')
                }
            )
        # then
        self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)
        self.assertRegex(str(response.data), 'authentication')


if __name__ == '__main__':
    unittest.main()
