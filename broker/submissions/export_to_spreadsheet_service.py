from ingest.api.ingestapi import IngestApi
from ingest.downloader.data_collector import DataCollector
from ingest.downloader.downloader import XlsDownloader


class ExportToSpreadsheetService:

    def __init__(self, ingest_api: IngestApi = None):
        self.ingest_api = IngestApi() if not ingest_api else ingest_api
        self.data_collector = DataCollector(self.ingest_api)
        self.downloader = XlsDownloader()

    def export(self, submission_uuid: str):
        self.ingest_api.unset_token()
        entity_dict = self.data_collector.collect_data_by_submission_uuid(submission_uuid)
        entity_list = list(entity_dict.values())
        flattened_json = self.downloader.convert_json(entity_list)
        workbook = self.downloader.create_workbook(flattened_json)
        return workbook
