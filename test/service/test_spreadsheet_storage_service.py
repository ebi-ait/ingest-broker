import json
import os
from os import path, mkdir
from os.path import basename
from tempfile import NamedTemporaryFile as temp_file
from tempfile import TemporaryFile
from tempfile import TemporaryDirectory
from unittest import TestCase

from broker.service.spreadsheet_storage.spreadsheet_storage_service import SpreadsheetStorageService


class _TestFile:

    def __init__(self, delegate_file: TemporaryFile):
        self.delegate_file = delegate_file

    def write(self, data):
        """
        Convenience utility so that the call to seek can be hidden.
        """
        self.delegate_file.write(data)
        self.delegate_file.seek(0)


def _as_test_file(test_file):
    return _TestFile(test_file)

class _TestDirectory:

    def __init__(self, directory_path):
        self.directory_path = directory_path

    def list_files(self, ends_with=None):
        if path.isdir(self.directory_path):
            files = os.listdir(self.directory_path)
            if ends_with:
                files = list(filter(lambda file: file.endswith(ends_with), files))
            return files
        else:
            raise RuntimeError(f'{self.directory_path} is not a directory')


def _as_test_dir(test_directory):
    return _TestDirectory(test_directory)


class SpreadsheetStorageServiceTest(TestCase):

    def test_store_spreadsheet(self):
        with TemporaryDirectory() as storage_root:
            # given:
            spreadsheet_storage_service = SpreadsheetStorageService(storage_root)

            # and:
            submission_uuid = "78451f36-c782-4d2d-8491-47e06ddb860f"
            file_name = "test_spreadsheet.xls"
            spreadsheet_data = bytes.fromhex('6d6f636b64617461')
            print()

            # when:
            spreadsheet_storage_service.store(submission_uuid, file_name, spreadsheet_data)

            # then:
            spreadsheet_directory = path.join(storage_root, submission_uuid)
            self.assertTrue('Spreadsheet directory does not exist.',
                            path.exists(spreadsheet_directory))

            # and:
            spreadsheet_files = os.listdir(spreadsheet_directory)
            self.assertTrue(file_name in spreadsheet_files)

    def test_store_updated_spreadsheet(self):
        with TemporaryDirectory() as storage_root:
            # given:
            storage = SpreadsheetStorageService(storage_root)
            submission_uuid = '44858abe-3d6a-4e86-aeed-0eb5891da6cf'
            file_name = 'submission.xlsx'
            storage.store(submission_uuid, file_name, b'spreadsheet')

            # and: assume file is stored
            spreadsheet_directory = _as_test_dir(path.join(storage_root, submission_uuid))
            self.assertEqual(1, len(spreadsheet_directory.list_files(ends_with='.xlsx')))

            # when: upload the same file again
            storage.store(submission_uuid, file_name, b'updated_spreadsheet')

            # then:
            self.assertEqual(2, len(spreadsheet_directory.list_files(ends_with='.xlsx')))

    def test_retrieve_spreadsheet(self):
        with TemporaryDirectory() as test_storage_dir:
            submission_uuid = "8e9602a9-b619-4593-ae68-3cc0f2cdf729"
            spreadsheet_dir = path.join(test_storage_dir, submission_uuid)
            mkdir(spreadsheet_dir)
            with temp_file(dir=spreadsheet_dir, suffix='.xls') as spreadsheet_file, \
                    temp_file(dir=spreadsheet_dir, suffix='.json', mode='w+') as manifest_file:
                # given:
                spreadsheet_data = bytes.fromhex('6d6f636b64617461')
                _as_test_file(spreadsheet_file).write(spreadsheet_data)

                # and: write content to file system
                file_name = basename(spreadsheet_file.name)
                manifest_file_name = basename(manifest_file.name)
                manifest_json = json.dumps({'name': file_name, 'location': spreadsheet_file.name})
                _as_test_file(manifest_file).write(manifest_json)

                # and:
                storage = SpreadsheetStorageService(test_storage_dir,
                                                    storage_manifest_name=manifest_file_name)

                # when:
                spreadsheet = storage.retrieve(submission_uuid)

                # then:
                self.assertEqual(file_name, spreadsheet['name'])
                self.assertEqual(spreadsheet_data, spreadsheet['blob'])
