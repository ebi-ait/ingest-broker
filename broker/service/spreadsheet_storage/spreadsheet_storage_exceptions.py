class SubmissionSpreadsheetAlreadyExists(Exception):
    pass


class SubmissionSpreadsheetDoesntExist(Exception):
    def __init__(self, submission_uuid: str, missing_path: str):
        self.submission_uuid = submission_uuid
        self.missing_path = missing_path
    pass
