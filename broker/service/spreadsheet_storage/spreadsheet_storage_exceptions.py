class SubmissionSpreadsheetAlreadyExists(Exception):
    pass


class SubmissionSpreadsheetDoesntExist(Exception):
    def __init__(self, submission_uuid: str, missing_path: str):
        super().__init__(f'No spreadsheet found for submission with uuid {submission_uuid}')
        self.submission_uuid = submission_uuid
        self.missing_path = missing_path
    pass
