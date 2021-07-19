from unittest import TestCase, skip

import requests
from ingest.api.ingestapi import IngestApi
from ingest.downloader.data_collector import DataCollector
from ingest.downloader.downloader import XlsDownloader
from ingest.importer.importer import XlsImporter

# TODO test the following flow:
# import new spreadsheet -> download spreadsheet -> update spreadsheet -> import updated spreadsheet


class BulkUpdateTest(TestCase):
    def setUp(self) -> None:
        self.ingest_api = IngestApi()
        self.importer = XlsImporter(self.ingest_api)
        self.submission_url = None
        self.data_collector = DataCollector(self.ingest_api)
        self.downloader = XlsDownloader()

    def test_download_spreadsheet(self):
        # download the test spreadsheet for the new submission

        # create a submission, track the submission url
        self.submission_url = 'http://localhost:8080/submissionEnvelopes/60f570c9a8a292649e814171'

        # download a spreadsheet
        entity_list = self.data_collector.collect_data_by_submission_uuid('98ec9cbb-e76c-4f8f-9b97-98fce7b151cb')
        flattened_json = self.downloader.convert_json(entity_list)
        workbook = self.downloader.create_workbook(flattened_json)
        workbook.save('downloaded-spreadsheet-original.xlsx')

        # assert spreadsheet contains data from submission

        pass

    @skip('only testing downloading for now')
    def test_upload_spreadsheet_updates(self):
        # download a spreadsheet given a submission uuid

        # modify a metadata the spreadsheet

        # import the modified spreadsheet
        path = 'downloaded-spreadsheet.xlsx'  # this is manually modified atm

        self.importer.import_file(path, self.submission_url, is_update=True)

        # get the modified metadata
        self.biomaterial = requests.get('http://localhost:8080/biomaterials/60f570d7a8a292649e814174').json()

        # assert that the update has been applied on the metadata
        self.assertEqual(self.biomaterial['content']['biomaterial_core']['biomaterial_description'], 'UPDATED')

