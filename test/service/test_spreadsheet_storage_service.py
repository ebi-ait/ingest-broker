import os
from tempfile import TemporaryDirectory
from unittest import TestCase

from broker.service.spreadsheet_storage.spreadsheet_storage_service import SpreadsheetStorageService


class SpreadsheetStorageServiceTest(TestCase):

    def test_store_spreadsheet(self):
        with TemporaryDirectory() as storage_root:
            # given:
            spreadsheet_storage_service = SpreadsheetStorageService(storage_root)

            # and:
            submission_uuid = "78451f36-c782-4d2d-8491-47e06ddb860f"
            file_name = "test_spreadsheet.xls"
            spreadsheet_data = bytes.fromhex('6d6f636b64617461')

            # when:
            spreadsheet_storage_service.store(submission_uuid, file_name, spreadsheet_data)

            # then:
            spreadsheet_directory = f'{storage_root}/{submission_uuid}'
            self.assertTrue('Spreadsheet directory does not exist.',
                            os.path.exists(spreadsheet_directory))

            # and:
            spreadsheet_files = os.listdir(spreadsheet_directory)
            self.assertTrue(file_name in spreadsheet_files)

    def test_retrieve_spreadsheet(self):
        with TemporaryDirectory() as test_storage_dir:
            # given:
            submission_uuid = "8e9602a9-b619-4593-ae68-3cc0f2cdf729"
            file_name = "spreadsheet.xls"
            spreadsheet_data = bytes.fromhex('6d6f636b64617461')
            spreadsheet_storage_service = SpreadsheetStorageService(test_storage_dir)

            # when:
            spreadsheet_storage_service.store(submission_uuid, file_name, spreadsheet_data)
            spreadsheet = spreadsheet_storage_service.retrieve(submission_uuid)

            # then:
            self.assertEqual(file_name, spreadsheet['name'])
            self.assertEqual(spreadsheet_data, spreadsheet['blob'])
