from http import HTTPStatus

class ImportSCEAHttpError(Exception):
    def __init__(self, message, status_code=None):
        super().__init__()
        self.message = message
        self.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
        if status_code is not None:
            self.status_code = status_code

class GenerateSCEAFilesError(ImportSCEAHttpError):
    def __init__(self, message):
        super().__init__(message, HTTPStatus.INTERNAL_SERVER_ERROR)
