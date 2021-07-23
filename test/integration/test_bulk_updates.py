import time
from unittest import TestCase

import requests
from ingest.api.ingestapi import IngestApi
from ingest.downloader.data_collector import DataCollector
from ingest.downloader.downloader import XlsDownloader
from ingest.importer.importer import XlsImporter
from openpyxl.worksheet.worksheet import Worksheet

from broker.submissions import ExportToSpreadsheetService

# TODO test the following flow:
# import new spreadsheet -> download spreadsheet -> update spreadsheet -> import updated spreadsheet


class BulkUpdateTest(TestCase):
    def setUp(self) -> None:
        # TODO add configuration to get ingest url from env variables
        # TODO currently it is hardcode for dev API
        self.ingest_url = "https://api.ingest.dev.archive.data.humancellatlas.org"
        self.ingest_api = IngestApi(self.ingest_url)
        # self.ingest_api = IngestApi()

        self.export_service = ExportToSpreadsheetService(self.ingest_api)

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

    def test_import_modify_and_export_spreadsheet_flow(self):
        # TODO: add JWT token for auth - without it the import won't work
        # download a spreadsheet given a submission uuid /<submission_uuid>/spreadsheet
        submission_uuid = 'e9e923d8-30b5-4184-87c5-fae40ce01ac9'
        submission = self.ingest_api.get_submission_by_uuid(submission_uuid)
        submission_id = submission['_links']['self']['href'].split('/')[-1]
        path_to_spreadsheet, spreadsheet = self.__export_spreadsheet(submission_uuid)

        # validate same cell values and then modify it then check it again that it has been modified
        project_sheet: Worksheet = spreadsheet.get_sheet_by_name('Project')
        updated_project_title, modified_project_id = self.__update_project_title(project_sheet)

        specimen_sheet: Worksheet = spreadsheet.get_sheet_by_name('Specimen from organism')
        updated_biomaterial_name, modified_biomaterial_id = self.__update_biomaterial_name(specimen_sheet)

        self.__save_modified_spreadsheet(path_to_spreadsheet, spreadsheet)

        # import the modified spreadsheet
        submission_url = self.ingest_url + '/submissionEnvelopes/' + submission_id
        self.importer.import_file(path_to_spreadsheet, submission_url, is_update=True)

        # get the modified metadata
        updated_project_from_api = requests.get(self.ingest_url + '/projects/' + modified_project_id).json()
        updated_biomaterial_from_api = requests.get(self.ingest_url + '/biomaterials/' + modified_biomaterial_id).json()

        # assert that the update has been applied on the metadata
        self.assertEqual(
            updated_project_from_api['content']['project_core']['project_title'], updated_project_title)
        self.assertEqual(
            updated_biomaterial_from_api['content']['biomaterial_core']['biomaterial_name'], updated_biomaterial_name)

    def __export_spreadsheet(self, submission_uuid):
        spreadsheet = self.export_service.export(submission_uuid)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        # temp_file = tempfile.NamedTemporaryFile()
        filename = f'{submission_uuid}_{timestamp}.xlsx'
        path_to_spreadsheet = 'temp_resources/' + filename
        spreadsheet.save(path_to_spreadsheet)

        return path_to_spreadsheet, spreadsheet

    def __update_project_title(self, project_sheet):
        orig_project_title = project_sheet.cell(6, 7).value
        updated_project_title = orig_project_title + ' UPDATED'
        project_sheet.cell(6, 7, updated_project_title)

        self.assertEqual(updated_project_title, project_sheet.cell(6, 7).value)

        modified_project_id = self.__get_entity_id(project_sheet, 'projects')

        return updated_project_title, modified_project_id

    def __update_biomaterial_name(self, specimen_sheet):
        orig_biomaterial_name = specimen_sheet.cell(6, 3).value
        updated_biomaterial_name = orig_biomaterial_name + ' UPDATED'
        specimen_sheet.cell(6, 3, updated_biomaterial_name)

        self.assertEqual(updated_biomaterial_name, specimen_sheet.cell(6, 3).value)

        modified_biomaterial_id = self.__get_entity_id(specimen_sheet, 'biomaterials')

        return updated_biomaterial_name, modified_biomaterial_id

    def __get_entity_id(self, sheet, entity_type):
        entity_uuid = sheet.cell(6, 1).value
        modified_entity = self.ingest_api.get_entity_by_uuid(entity_type, entity_uuid)
        return modified_entity['_links']['self']['href'].split('/')[-1]

    @staticmethod
    def __save_modified_spreadsheet(path_to_spreadsheet, spreadsheet):
        spreadsheet.save(path_to_spreadsheet)
