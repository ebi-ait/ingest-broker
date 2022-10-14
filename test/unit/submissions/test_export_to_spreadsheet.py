import json
import unittest
from http import HTTPStatus
from unittest.mock import patch, Mock

from openpyxl.workbook import Workbook

from broker.submissions import ExportToSpreadsheetService
from test.unit.test_broker_app import BrokerAppTest


class RouteTestCase(BrokerAppTest):
    """
    tests the submissions route
    """
    def setUp(self):
        super().setUp()
        self._app.SPREADSHEET_STORAGE_DIR = 'mock_storage_dir'
        self.mock_submission = {
            'lastSpreadsheetGenerationJob': {
                'finishedDate': '2022-01-27T12:00:58.417Z',
                'createdDate': '2022-01-27T11:57:05.187Z'
            }
        }
        self.mock_ingest.get_submission_by_uuid = Mock(return_value=self.mock_submission)

    @patch('broker.submissions.routes.send_file')
    def test_download_spreadsheet__success(self, mock_send_file):
        # given
        mock_send_file.return_value = self._app.response_class(
            response=json.dumps({}),
            status=HTTPStatus.OK,
            mimetype='application/json'
        )

        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_send_file.assert_called_once()

    @patch('broker.submissions.routes.send_file')
    def test_download_spreadsheet__not_found(self, mock_send_file):
        # given
        self.mock_submission = {}
        self.mock_ingest.get_submission_by_uuid = Mock(return_value=self.mock_submission)

        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
        mock_send_file.assert_not_called()

    @patch('broker.submissions.routes.send_file')
    def test_download_spreadsheet__accepted(self, mock_send_file):
        # given
        self.mock_submission = {
            'lastSpreadsheetGenerationJob': {
                'finishedDate': None,
                'createdDate': '2022-01-27T11:57:05.187Z'
            }
        }
        self.mock_ingest.get_submission_by_uuid = Mock(return_value=self.mock_submission)
        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.get(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.ACCEPTED)
        mock_send_file.assert_not_called()

    @patch.object(ExportToSpreadsheetService,'async_export_and_save',
                  return_value='test-job-id')
    @patch('broker.submissions.routes.IngestApi')
    def test_generate_spreadsheet__accepted__finished(self,
                                                      mock_async_export_and_save,
                                                      mock_ingest_api_authenticated):
        '''
        tests the happy path for the controller
        '''
        # given
        self.mock_submission = {
            'lastSpreadsheetGenerationJob': {
                'createdDate': '2022-01-27T11:57:05.187Z',
                'finishedDate': '2022-01-27T12:00:58.417Z'
            }
        }
        submission_uuid = 'xyz-001'
        with self._app.test_client() as app:
            # when
            response = app.post(f'/submissions/{submission_uuid}/spreadsheet',
                                headers={'Authorization': 'test'})

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(HTTPStatus.ACCEPTED, response.status_code)

        mock_async_export_and_save.assert_called_once()
        # ToDo: Build correct mocking of calls-to and responses-from ingest


    def test_no_auth_header__bad_request(self):
        submission_uuid = 'xyz-001'
        with self._app.test_client() as app:
            # when
            response = app.post(f'/submissions/{submission_uuid}/spreadsheet')
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)


    @patch('broker.submissions.ExportToSpreadsheetService.async_export_and_save',
           ExportToSpreadsheetService.export_and_save)
    def test_generate_spreadsheet__accepted__not_created(self):
        # given
        self.mock_submission = {
            'lastSpreadsheetGenerationJob': None
        }
        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.ACCEPTED)
        # ToDo: Build correct mocking of calls-to and responses-from ingest

    @patch('broker.submissions.ExportToSpreadsheetService.async_export_and_save',
           ExportToSpreadsheetService.export_and_save)
    def test_generate_spreadsheet__accepted__already_created(self):
        # given
        self.mock_submission = {
            'lastSpreadsheetGenerationJob': {
                'createdDate': '2022-01-27T11:57:05.187Z',
                'finishedDate': None
            }
        }
        self.mock_ingest.get_submission_by_uuid = Mock(return_value=self.mock_submission)
        submission_uuid = 'xyz-001'

        with self._app.test_client() as app:
            # when
            response = app.post(f'/submissions/{submission_uuid}/spreadsheet')

        # then
        self.mock_ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        self.assertEqual(response.status_code, HTTPStatus.ACCEPTED)

class ServiceTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.mock_submission = self.create_test_submission()
        self.mock_project = self.create_test_project()

        self.mock_ingest = Mock()
        self.mock_ingest.get_submission_by_uuid = Mock(return_value=self.mock_submission)
        # get_related_entities returns a generator
        self.mock_ingest.get_related_entities = Mock(return_value=(_ for _ in [self.mock_project]))

    def create_test_project(self):
        return {}

    def create_test_submission(self):
        return {
            '_links': {
                'self': {'href': 'http://test.submission/'},
                'projects': {'href': 'http://ingest/submissionEnvelopes/test-submission-uuid/projects'}
            },
            'stagingDetails': {'stagingAreaLocation': {'value': 's3://upload-bucket/project-uuid'}}
        }

    def test_spreadsheet_generation(self):
        # given
        service = self.setup_service()
        s3_client = Mock()
        service.init_s3_client = Mock(return_value=s3_client)

        # when
        service.export_and_save(submission_uuid='test-submission-uuid',
                                storage_dir='spreadsheets',
                                job_id='job-id')

        # then
        # check whatever you need to check to make sure

        # check s3 client invoked correctly
        s3_client.upload_file.assert_called_once()
        # todo: check call parameters

        # check the project is linked properly
        call_args_list = self.mock_ingest.link_entity.call_args_list

    def setup_service(self):
        service = ExportToSpreadsheetService(self.mock_ingest)
        service.config = {'AWS_ACCESS_KEY_ID': 'test', 'AWS_ACCESS_KEY_SECRET': 'test'}
        # actual spreadsheet generation is not part of this test
        service.export = Mock(return_value=Workbook())
        return service


if __name__ == '__main__':
    unittest.main()
