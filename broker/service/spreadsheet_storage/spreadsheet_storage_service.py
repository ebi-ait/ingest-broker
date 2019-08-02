import os
import json
from os import path

from .spreadsheet_storage_exceptions import SubmissionSpreadsheetAlreadyExists, SubmissionSpreadsheetDoesntExist


class SpreadsheetStorageService:

    def __init__(self, storage_dir, storage_manifest_name="storage_manifest.json"):
        self.storage_dir = storage_dir
        self.storage_manifest_name = storage_manifest_name

    def store(self, submission_uuid, spreadsheet_name, spreadsheet_blob):
        """
        Stores a given spreadsheet at path <submission_uuid>/<spreadsheetname>, local to
        the storage directory
        :param submission_uuid:
        :param spreadsheet_name:
        :param spreadsheet_blob:
        :return:
        """
        submission_dir = path.join(self.storage_dir, submission_uuid)
        if not path.exists(submission_dir):
            os.mkdir(submission_dir)
        try:
            submission_spreadsheet_path = path.join(submission_dir, spreadsheet_name)
            index = 0
            while path.exists(submission_spreadsheet_path):
                submission_spreadsheet_path = path.join(submission_dir,
                                                        f'{spreadsheet_name}{index}.xlsx')
                index += 1
            storage_manifest_path = path.join(submission_dir, self.storage_manifest_name)
            with open(submission_spreadsheet_path, "wb") as spreadsheet_file:
                spreadsheet_file.write(spreadsheet_blob)
                with open(storage_manifest_path, "w") as storage_manfiest:
                    json.dump({"name": spreadsheet_name, "location": submission_spreadsheet_path}, storage_manfiest)
                    return submission_spreadsheet_path
        except FileExistsError:
            raise SubmissionSpreadsheetAlreadyExists()

    def retrieve(self, submission_uuid):
        try:
            spreadsheet_location = self.get_spreadsheet_location(submission_uuid)
            spreadsheet_name = spreadsheet_location["name"]
            spreadsheet_path = spreadsheet_location["path"]
            with open(spreadsheet_path, "rb") as spreadsheet_file:
                spreadsheet_blob = spreadsheet_file.read()
                return {"name": spreadsheet_name, "blob": spreadsheet_blob}
        except SubmissionSpreadsheetDoesntExist as e:
            raise e

    def get_spreadsheet_location(self, submission_uuid):
        submission_dir_path = f'{self.storage_dir}/{submission_uuid}'
        storage_manifest_path = f'{submission_dir_path}/{self.storage_manifest_name}'
        if not os.path.isdir(submission_dir_path):
            raise SubmissionSpreadsheetDoesntExist()
        else:
            if not os.path.isfile(storage_manifest_path):
                raise SubmissionSpreadsheetDoesntExist()
            else:
                with open(storage_manifest_path, "rb") as storage_manifest_file:
                    storage_manifest = json.load(storage_manifest_file)
                    spreadsheet_name = storage_manifest["name"]
                    spreadsheet_path = storage_manifest["location"]
                    if not os.path.isfile(spreadsheet_path):
                        raise SubmissionSpreadsheetDoesntExist()
                    else:
                        return {"name": spreadsheet_name, "path": spreadsheet_path}
