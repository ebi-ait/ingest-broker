import datetime
import logging
import os
import threading
from collections import namedtuple
from datetime import datetime, timezone

from ingest.api.ingestapi import IngestApi
from ingest.downloader.data_collector import DataCollector
from ingest.downloader.downloader import XlsDownloader
from ingest.utils.date import date_to_json_string


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

        spreadsheet_details = self.get_spreadsheet_details(create_date, storage_dir, submission_uuid)

        workbook = self.export(submission_uuid)

        os.makedirs(spreadsheet_details.directory, exist_ok=True)
        workbook.save(spreadsheet_details.filepath)

        finished_date = datetime.now(timezone.utc)
        self._patch(submission_url, create_date, finished_date)

        self.logger.info(f'Done exporting spreadsheet for submission {submission_uuid}!')

    @staticmethod
    def get_spreadsheet_details(create_date, storage_dir, submission_uuid):
        directory = f'{storage_dir}/{submission_uuid}/downloads/'
        timestamp = create_date.strftime("%Y%m%d-%H%M%S")
        filename = f'{submission_uuid}_{timestamp}.xlsx'
        filepath = f'{directory}/{filename}'

        SpreadsheetDetails = namedtuple("File", "filename filepath directory")
        details = SpreadsheetDetails(filename, filepath, directory)
        return details

    def _patch(self, submission_url, create_date, finished_date=None):
        patch = {
            'lastSpreadsheetGenerationJob': {
                'finishedDate': date_to_json_string(finished_date) if finished_date else None,
                'createdDate': date_to_json_string(create_date)
            }
        }
        self.ingest_api.patch(submission_url, patch)

    def async_export_and_save(self, submission_uuid: str, storage_dir: str):
        thread = threading.Thread(target=self.export_and_save, args=(submission_uuid, storage_dir))
        thread.start()
