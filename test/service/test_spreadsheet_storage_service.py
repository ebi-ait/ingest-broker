import os
import shutil
from tempfile import TemporaryDirectory
from unittest import TestCase

from broker.service.spreadsheet_storage.spreadsheet_storage_service import SpreadsheetStorageService

TEST_STORAGE_DIR = "test_storage_dir"


class SpreadsheetStorageServiceTest(TestCase):

    def setUp(self):
        os.mkdir(TEST_STORAGE_DIR)

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
        test_storage_dir = "test_storage_dir"
        mock_submission_uuid = "mock-uuid"
        mock_spreadsheet_name = "mock_spreadsheet.xls"
        mock_spreadsheet_blob = bytes.fromhex('6d6f636b64617461')
        spreadsheet_storage_service = SpreadsheetStorageService(test_storage_dir)

        try:
            spreadsheet_storage_service.store(mock_submission_uuid, mock_spreadsheet_name,
                                              mock_spreadsheet_blob)
            spreadsheet = spreadsheet_storage_service.retrieve(mock_submission_uuid)
            assert spreadsheet["name"] == mock_spreadsheet_name
            assert spreadsheet["blob"] == mock_spreadsheet_blob
        except Exception as e:
            assert False

    def tearDown(self):
        shutil.rmtree(TEST_STORAGE_DIR)
