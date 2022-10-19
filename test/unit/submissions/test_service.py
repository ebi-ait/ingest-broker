import uuid
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import Mock

import boto3
from hca_ingest.api.ingestapi import IngestApi
from openpyxl import Workbook

from broker.submissions import ExportToSpreadsheetService


BUCKET = 'upload-bucket'
STORAGE_DIR = 'spreadsheets'
SCHEMA_URL = 'https://schema/type/file/version/supplementary_file'


class ServiceTestCase(TestCase):
    def setUp(self):
        self.ingest = Mock(spec=IngestApi)
        self.s3 = Mock(Spec=boto3.client)

    def test_spreadsheet_generation(self):
        # Given
        start_date = datetime.now(timezone.utc)
        job_id = str(uuid.uuid4())
        project = self.mock_project()
        submission = self.mock_submission()
        submission_url = submission['_links']['self']['href']
        submission_uuid = submission['uuid']['uuid']
        details = ExportToSpreadsheetService.get_spreadsheet_details(start_date, STORAGE_DIR, submission_uuid)
        file = ExportToSpreadsheetService.build_supplementary_file_payload(SCHEMA_URL, details.filename)
        s3_key = f'{submission_uuid}/{details.filename}'
        service = self.partial_mock_service(self.ingest, self.s3, start_date)
        self.ingest.get_submission_by_uuid = Mock(return_value=submission)
        self.ingest.get_related_entities = Mock(return_value=(_ for _ in [project]))
        self.ingest.get_latest_schema_url = Mock(return_value=SCHEMA_URL)
        self.ingest.create_file = Mock(return_value=file)
        self.ingest.link_entity = Mock()

        # when
        service.export_and_save(
            submission_uuid=submission_uuid,
            storage_dir=STORAGE_DIR,
            job_id=job_id
        )

        # then
        self.ingest.get_submission_by_uuid.assert_called_once_with(submission_uuid)
        service.update_spreadsheet_start.assert_called_once_with(submission_url, job_id)
        service.export.assert_called_once_with(submission_uuid)
        self.ingest.create_file.assert_called_once_with(submission_url, filename=details.filename, content=file)
        self.ingest.get_related_entities.assert_called_once_with(entity=submission, relation='projects', entity_type='projects')
        self.ingest.link_entity.assert_called_once_with(from_entity=project, to_entity=file, relationship='supplementaryFiles')
        service.update_spreadsheet_finish.assert_called_once_with(start_date, submission_url, job_id)
        self.s3.upload_file.assert_called_once_with(Filename=details.filepath, Bucket=BUCKET, Key=s3_key)

    @staticmethod
    def partial_mock_service(ingest: IngestApi, s3, start_date: datetime) -> ExportToSpreadsheetService:
        service = ExportToSpreadsheetService(ingest)
        service.config = {'AWS_ACCESS_KEY_ID': 'test', 'AWS_ACCESS_KEY_SECRET': 'test'}
        # actual spreadsheet generation is not part of this test
        service.export = Mock(return_value=Workbook())
        service.save_spreadsheet = Mock()
        service.update_spreadsheet_start = Mock(return_value=start_date)
        service.update_spreadsheet_finish = Mock()
        service.init_s3_client = Mock(return_value=s3)
        return service

    @staticmethod
    def mock_project():
        return {
            'uuid': {
                'uuid': str(uuid.uuid4())
            }
        }

    @staticmethod
    def mock_submission():
        submission_uuid = str(uuid.uuid4())
        submission_id = str(uuid.uuid4())
        return {
            'uuid': {
                'uuid': submission_uuid
            },
            '_links': {
                'self': {'href': f'http://ingest/submissionEnvelopes/{submission_id}/'},
                'projects': {'href': f'http://ingest/submissionEnvelopes/{submission_id}/projects'}
            },
            'stagingDetails': {'stagingAreaLocation': {'value': f's3://{BUCKET}/{submission_uuid}'}}
        }