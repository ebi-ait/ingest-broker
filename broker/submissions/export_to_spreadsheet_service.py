from ingest.api.ingestapi import IngestApi


class ExportToSpreadsheetService:

    def __init__(self, ingest_api=None):
        self.ingestapi = IngestApi() if not ingest_api else ingest_api

    def export(self, submission_uuid:str):
        return dict(submission_uuid=submission_uuid, status='not_implemented')
