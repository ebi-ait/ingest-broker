import logging
import os
import threading
import uuid
from collections import namedtuple
from datetime import datetime, timezone

from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.downloader.data_collector import DataCollector
from hca_ingest.downloader.downloader import XlsDownloader
from hca_ingest.utils.date import date_to_json_string

SpreadsheetDetails = namedtuple("SpreadsheetDetails", "filename filepath directory")


class ExportToSpreadsheetService:

    def __init__(self, ingest_api: IngestApi, app=None):
        self.ingest_api = ingest_api
        self.data_collector = DataCollector(self.ingest_api)
        self.app = None
        self.config = None

        self.downloader = XlsDownloader()
        self.logger = logging.getLogger(__name__)
        if app:
            self.init_app(app)

    def init_app(self, app):
        """
        proper way to configure service classes used by flask
        https://flask.palletsprojects.com/en/2.1.x/extensions/
        :param app:
        :return:
        """
        self.app = app
        app_config = self.app.config
        self.configure(app_config)

    def configure(self, config):
        self.config = {}
        # self.config['key'] = config['key']

    def async_export_and_save(self, submission_uuid: str, storage_dir: str):
        job_id = str(uuid.uuid4())
        thread = threading.Thread(target=self.export_and_save, args=(submission_uuid, storage_dir, job_id))
        thread.start()
        return job_id

    def export_and_save(self, submission_uuid: str, storage_dir: str, job_id: str):
        self.logger.info(f'Exporting submission {submission_uuid}, job id = {job_id}')
        submission = self.ingest_api.get_submission_by_uuid(submission_uuid)
        try:
            submission_url = submission['_links']['self']['href']
            create_date = self.update_spreadsheet_start(submission_url, job_id)
            spreadsheet_details = self.get_spreadsheet_details(storage_dir, submission_uuid)
            file = self.update_submission_supplementary_file(submission_url, submission, spreadsheet_details.filename)
            workbook = self.export(submission_uuid)
            self.save_spreadsheet(spreadsheet_details, workbook)
            self.update_spreadsheet_finish(create_date, submission_url, job_id)
            self.logger.info(f'Done exporting spreadsheet for submission {submission_uuid}!')
        except Exception as e:
            err = f'Problem when generating spreadsheet for submission with uuid {submission_uuid}: {str(e)}'
            self.logger.error(err, e)
            raise Exception(err) from e

    def update_spreadsheet_start(self, submission_url, job_id):
        self.logger.info(f'Starting Spreadsheet Generation Job: {submission_url}, JobId: {job_id}')
        create_date = datetime.now(timezone.utc)
        self.__patch_file_generation(submission_url, create_date, job_id)
        return create_date

    def export(self, submission_uuid: str):
        self.logger.info(f'Generating Spreadsheet: {submission_uuid}')
        entity_dict = self.data_collector.collect_data_by_submission_uuid(submission_uuid)
        entity_list = list(entity_dict.values())
        flattened_json = self.downloader.convert_json(entity_list)
        workbook = self.downloader.create_workbook(flattened_json)
        return workbook

    def update_spreadsheet_finish(self, create_date: datetime, submission_url: str, job_id: str):
        finished_date = datetime.now(timezone.utc)
        self.__patch_file_generation(submission_url, create_date, job_id, finished_date)

    def __patch_file_generation(self, submission_url, create_date: datetime, job_id: str, finished_date=None):
        patch = self.build_generation_job(create_date, job_id, finished_date)
        self.ingest_api.patch(submission_url, json=patch)

    @staticmethod
    def get_spreadsheet_details(storage_dir, submission_uuid) -> SpreadsheetDetails:
        directory = f'{storage_dir}/{submission_uuid}/downloads'
        filename = f'{submission_uuid}.xlsx'
        filepath = f'{directory}/{filename}'
        return SpreadsheetDetails(filename, filepath, directory)

    @staticmethod
    def save_spreadsheet(spreadsheet_details: SpreadsheetDetails, workbook):
        os.makedirs(spreadsheet_details.directory, exist_ok=True)
        workbook.save(spreadsheet_details.filepath)

    @staticmethod
    def build_generation_job(create_date, job_id, finished_date):
        return {
            'lastSpreadsheetGenerationJob': {
                'finishedDate': date_to_json_string(finished_date) if finished_date else None,
                'createdDate': date_to_json_string(create_date),
                'jobId': job_id
            }
        }

    @staticmethod
    def is_file_valid(file: dict) -> bool:
        state = file.get('validationState', 'Invalid')
        return state == 'Valid'
