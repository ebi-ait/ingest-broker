import os
from unittest import TestCase

import polling as polling
import requests
from assertpy import assert_that


def is_spreadsheet_ready(submission):
    print('checking submission')
    return submission['lastSpreadsheetGenerationJob'].get('finishedDate','')



class TestMetadataSpreadsheetGenerator(TestCase):
    def test_spreadsheet_generation(self):
        ingest_url = 'https://api.ingest.dev.archive.data.humancellatlas.org'
        broker_url = 'http://localhost:5001'

        submission_uuid = 'c81f7d54-a27f-4212-a6df-88dde947f7cc'
        # generate spreadsheet
        token = os.environ['INGEST_TOKEN']
        generate_spreadsheet_response = requests.post(f'{broker_url}/submissions/{submission_uuid}/spreadsheet', headers={'Authorization': token})
        submission = requests.get(f'{ingest_url}/submissionEnvelopes/search/findByUuidUuid', params={'uuid':submission_uuid}).json()
        # wait
        polling.poll(
            lambda: requests.get(submission['_links']['self']['href']).json(),
            check_success=is_spreadsheet_ready,
            step=1,
            timeout=600)

        # check files added to submission
        self.check_file_in_submission(submission)

        # check supplementary files added to project
        self.check_file_in_related_project(submission)

    def check_file_in_related_project(self, submission):
        project = requests.get(submission['_links']['projcets']['href']).json()[0]
        supplementary_files = requests.get(project['_links']['supplementaryFiles']['href']).json()
        assert_that(supplementary_files).contains({'a': 'y'})

    def check_file_in_submission(self, submission):
        submission_files = requests.get(submission['_links']['files']['href']).json()
        excels = filter(lambda file: file['content']['file_core']['format'] == 'xlsx', submission_files['files'])
        assert_that(excels).is_not_empty()
