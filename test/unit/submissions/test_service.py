import unittest
from unittest.mock import Mock

from openpyxl import Workbook

from broker.submissions import ExportToSpreadsheetService


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
