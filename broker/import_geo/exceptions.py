from http import HTTPStatus


class ImportGeoHttpError(Exception):
    def __init__(self, message, status_code=None):
        super().__init__()
        self.message = message
        self.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
        if status_code is not None:
            self.status_code = status_code


class InvalidGeoAccession(ImportGeoHttpError):
    def __init__(self, message):
        super().__init__(message, HTTPStatus.BAD_REQUEST)


class GenerateGeoWorkbookError(ImportGeoHttpError):
    def __init__(self, message):
        super().__init__(message, HTTPStatus.INTERNAL_SERVER_ERROR)


class ImportProjectWorkbookError(ImportGeoHttpError):
    def __init__(self, message):
        super().__init__(message, HTTPStatus.INTERNAL_SERVER_ERROR)
