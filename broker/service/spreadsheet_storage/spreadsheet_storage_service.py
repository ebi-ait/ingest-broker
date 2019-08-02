from os import path

from .spreadsheet_storage_exceptions import SubmissionSpreadsheetAlreadyExists


class SpreadsheetStorageService:

    def __init__(self, storage_dir, storage_manifest_name="storage_manifest.json"):
        self.storage_dir = storage_dir
        self.storage_manifest_name = storage_manifest_name

    # TODO remove spreadsheet_name
    def store(self, submission_uuid, spreadsheet_name, spreadsheet_blob):
        """
        Stores a given spreadsheet at path <submission_uuid>/<spreadsheetname>, local to
        the storage directory
        :param submission_uuid:
        :param spreadsheet_name:
        :param spreadsheet_blob:
        :return:
        """
        try:
            file_path = path.join(self.storage_dir, f'{submission_uuid}.xlsx')
            with open(file_path, "wb") as spreadsheet_file:
                spreadsheet_file.write(spreadsheet_blob)
                return file_path
        except FileExistsError:
            raise SubmissionSpreadsheetAlreadyExists()

    def retrieve(self, submission_uuid):
        file_path = path.join(self.storage_dir, f'{submission_uuid}.xlsx')
        file_manifest = {'name': file_path}
        with open(file_path, 'rb') as spreadsheet_file:
            data = spreadsheet_file.read()
            file_manifest['blob'] = data
        return file_manifest
