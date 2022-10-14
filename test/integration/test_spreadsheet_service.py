from unittest import TestCase
from hca_ingest.api.ingestapi import IngestApi
from http import HTTPStatus

#from broker.submissions.export_to_spreadsheet_service import ExportToSpreadsheetService


class TestSpreadsheetService(TestCase):
    def setUp(self) -> None:
        # how can we import app here?
        self._app = None

    def export_and_save(self):
        ingest_url = "https://api.ingest.dev.archive.data.humancellatlas.org"
        ingest_api = IngestApi(ingest_url)
        #service = ExportToSpreadsheetService(ingest_api)
        #service.config = {'AWS_ACCESS_KEY_ID': 'test', 'AWS_ACCESS_KEY_SECRET': 'test'}
        #s3_client = Mock()
        #service.init_s3_client = Mock(return_value=s3_client)

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

        # tests the linkage
        # test the project status
