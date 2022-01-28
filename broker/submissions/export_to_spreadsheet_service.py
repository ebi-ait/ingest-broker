import datetime
import logging
import os
import threading
from datetime import datetime, timezone

from ingest.api.ingestapi import IngestApi
from ingest.downloader.data_collector import DataCollector
from ingest.downloader.downloader import XlsDownloader


class ExportToSpreadsheetService:

    def __init__(self, ingest_api: IngestApi):
        self.ingest_api = ingest_api
        self.data_collector = DataCollector(self.ingest_api)
        self.downloader = XlsDownloader()
        self.logger = logging.getLogger(__name__)

    def export(self, submission_uuid: str):
        self.ingest_api.unset_token()
        entity_dict = self.data_collector.collect_data_by_submission_uuid(submission_uuid)
        entity_list = list(entity_dict.values())
        flattened_json = self.downloader.convert_json(entity_list)
        workbook = self.downloader.create_workbook(flattened_json)
        return workbook

    def export_and_save(self, submission_uuid: str, storage_dir: str):
        self.logger.info(f'Exporting submission {submission_uuid}')
        try:
            submission = self.ingest_api.get_submission_by_uuid(submission_uuid)
            submission_url = submission['_links']['self']['href']
        except Exception as e:
            self.logger.error(e)
            raise Exception(f'An error occurred in retrieving the submission with uuid {submission_uuid}: {str(e)}')

        create_date = datetime.now(timezone.utc)

        self._patch(submission_url, create_date)

        directory = f'{storage_dir}/{submission_uuid}'
        os.makedirs(f'{directory}/downloads/', exist_ok=True)

        timestamp = create_date.strftime("%Y%m%d-%H%M%S")
        filename = f'{submission_uuid}_{timestamp}.xlsx'
        filepath = f'{directory}/downloads/{filename}'

        workbook = self.export(submission_uuid)
        workbook.save(filepath)

        finished_date = datetime.now(timezone.utc)
        self._patch(submission_url, create_date, finished_date)

        self.logger.info(f'Done exporting spreadsheet for submission {submission_uuid}!')

    def _patch(self, submission_url, create_date, finished_date=None):
        patch = {
            'lastSpreadsheetGenerationJob': {
                'finishedDate': finished_date.isoformat().replace("+00:00", "Z") if finished_date else None,
                'createdDate': create_date.isoformat().replace("+00:00", "Z")
            }
        }
        self.ingest_api.patch(submission_url, patch)

    def async_export_and_save(self, submission_uuid: str, storage_dir: str):
        thread = threading.Thread(target=self.export_and_save, args=(submission_uuid, storage_dir))
        thread.start()
