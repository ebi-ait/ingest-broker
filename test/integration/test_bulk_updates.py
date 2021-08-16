import time
from pathlib import Path
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
        # TODO I am going to move this test to the integration test package where I don't need to hardcode a JWT Token
        # TODO then all the above is going to be solved
        self.ingest_url = "https://api.ingest.dev.archive.data.humancellatlas.org"
        jwt_token = '<TOKEN>'
        self.ingest_api = IngestApi(self.ingest_url)
        self.ingest_api.set_token(jwt_token)

        self.export_service = ExportToSpreadsheetService(self.ingest_api)

        self.importer = XlsImporter(self.ingest_api)
        self.submission_url = None
        self.data_collector = DataCollector(self.ingest_api)
        self.downloader = XlsDownloader()
        self.path_to_spreadsheet = None
        self.modified_project_id = None
        self.modified_biomaterial_id = None
        self.original_project_from_api = None
        self.original_biomaterial_from_api = None

    def tearDown(self) -> None:
        self.remove_temp_worksheet()

        self.revert_original_content('projects', self.modified_project_id, self.original_project_from_api)
        self.revert_original_content('biomaterials', self.modified_biomaterial_id, self.original_biomaterial_from_api)

    def remove_temp_worksheet(self):
        sheet_path = Path(self.path_to_spreadsheet)
        sheet_path.unlink()

    def revert_original_content(self, entity_type, entity_id, original_content):
        self.ingest_api.patch(self.ingest_url + f'/{entity_type}/' + entity_id, original_content)

    def test_export__modify__import_spreadsheet_flow(self):
        # download a spreadsheet given a submission uuid /<submission_uuid>/spreadsheet
        submission_uuid = 'e9e923d8-30b5-4184-87c5-fae40ce01ac9'
        submission = self.ingest_api.get_submission_by_uuid(submission_uuid)
        submission_id = submission['_links']['self']['href'].split('/')[-1]
        self.path_to_spreadsheet, spreadsheet = self.__export_spreadsheet(submission_uuid)

        # validate same cell values and then modify it then check it again that it has been modified
        project_sheet: Worksheet = spreadsheet.get_sheet_by_name('Project')
        updated_project_title, self.modified_project_id = self.__update_project_title(project_sheet)

        specimen_sheet: Worksheet = spreadsheet.get_sheet_by_name('Specimen from organism')
        updated_biomaterial_name, self.modified_biomaterial_id = self.__update_biomaterial_name(specimen_sheet)

        self.__save_modified_spreadsheet(self.path_to_spreadsheet, spreadsheet)

        # get the original metadata
        self.original_project_from_api = self.__get_entity_json_by_type_and_id('projects', self.modified_project_id)
        self.original_biomaterial_from_api = self.__get_entity_json_by_type_and_id('biomaterials', self.modified_biomaterial_id)

        # import the modified spreadsheet
        submission_url = self.ingest_url + '/submissionEnvelopes/' + submission_id
        self.importer.import_file(self.path_to_spreadsheet, submission_url, is_update=True)

        # get the modified metadata
        updated_project_from_api = self.__get_entity_json_by_type_and_id('projects', self.modified_project_id)
        updated_biomaterial_from_api = self.__get_entity_json_by_type_and_id('biomaterials', self.modified_biomaterial_id)

        # assert that the update has been applied on the metadata
        self.assertEqual(
            updated_project_title, updated_project_from_api['content']['project_core']['project_title'])
        self.assertEqual(
            updated_biomaterial_name, updated_biomaterial_from_api['content']['biomaterial_core']['biomaterial_name'])

    def __get_entity_json_by_type_and_id(self, entity_type, entity_id):
        return requests.get(self.ingest_url + f'/{entity_type}/' + entity_id).json()

    def __export_spreadsheet(self, submission_uuid):
        spreadsheet = self.export_service.export(submission_uuid)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        filename = f'{submission_uuid}_{timestamp}.xlsx'
        path_to_spreadsheet = 'temp_resources/' + filename
        spreadsheet.save(path_to_spreadsheet)

        return path_to_spreadsheet, spreadsheet

    def __update_project_title(self, project_sheet):
        orig_project_title = project_sheet.cell(6, 7).value
        updated_project_title = orig_project_title + ' UPDATED 999'
        project_sheet.cell(6, 7, updated_project_title)

        self.assertEqual(updated_project_title, project_sheet.cell(6, 7).value)

        modified_project_id = self.__get_entity_id(project_sheet, 'projects')

        return updated_project_title, modified_project_id

    def __update_biomaterial_name(self, specimen_sheet):
        orig_biomaterial_name = specimen_sheet.cell(6, 4).value
        updated_biomaterial_name = orig_biomaterial_name + ' UPDATED 123'
        specimen_sheet.cell(6, 4, updated_biomaterial_name)

        self.assertEqual(updated_biomaterial_name, specimen_sheet.cell(6, 4).value)

        modified_biomaterial_id = self.__get_entity_id(specimen_sheet, 'biomaterials')

        return updated_biomaterial_name, modified_biomaterial_id

    def __get_entity_id(self, sheet, entity_type):
        entity_uuid = sheet.cell(6, 1).value
        modified_entity = self.ingest_api.get_entity_by_uuid(entity_type, entity_uuid)
        return modified_entity['_links']['self']['href'].split('/')[-1]

    @staticmethod
    def __save_modified_spreadsheet(path_to_spreadsheet, spreadsheet):
        spreadsheet.save(path_to_spreadsheet)
