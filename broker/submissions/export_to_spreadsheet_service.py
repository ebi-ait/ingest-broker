import logging
import os
import threading
import uuid
from collections import namedtuple
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.downloader.data_collector import DataCollector
from hca_ingest.downloader.downloader import XlsDownloader
from hca_ingest.utils.date import date_to_json_string


SpreadsheetDetails = namedtuple("SpreadsheetDetails", "filename filepath directory")


class ExportToSpreadsheetService:

    def __init__(self, ingest_api: IngestApi, app=None):
        self.ingest_api:IngestApi = ingest_api
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
        self.config = {
            "AWS_ACCESS_KEY_ID": config['AWS_ACCESS_KEY_ID'],
            "AWS_ACCESS_KEY_SECRET": config['AWS_ACCESS_KEY_SECRET']
        }

    def async_export_and_save(self, submission_uuid: str, storage_dir: str):
        job_id = str(uuid.uuid4())
        thread = threading.Thread(target=self.export_and_save, args=(submission_uuid, storage_dir, job_id))
        thread.start()
        return job_id

    def export_and_save(self, submission_uuid: str, storage_dir: str, job_id: str):
        self.logger.info(f'Exporting submission {submission_uuid}, job id = {job_id}')
        try:
            submission = self.ingest_api.get_submission_by_uuid(submission_uuid)
            submission_url = submission['_links']['self']['href']
            staging_area = submission['stagingDetails']['stagingAreaLocation']['value']
        except Exception as e:
            self.logger.error(e)
            raise Exception(f'An error occurred in retrieving the submission with uuid {submission_uuid}: {str(e)}') from e

        create_date = self.update_spreadsheet_start(submission_url, job_id)
        spreadsheet_details = self.get_spreadsheet_details(create_date, storage_dir, submission_uuid)
        workbook = self.export(submission_uuid)
        self.save_spreadsheet(spreadsheet_details, workbook)
        self.link_spreadsheet(submission_url, submission, spreadsheet_details.filename)
        self.update_spreadsheet_finish(create_date, submission_url, job_id)
        self.copy_to_s3_staging_area(spreadsheet_details, staging_area)
        self.logger.info(f'Done exporting spreadsheet for submission {submission_uuid}!')

    def update_spreadsheet_start(self, submission_url, job_id):
        create_date = datetime.now(timezone.utc)
        self.__patch_file_generation(submission_url, create_date, job_id)
        return create_date

    def export(self, submission_uuid: str):
        entity_dict = self.data_collector.collect_data_by_submission_uuid(submission_uuid)
        entity_list = list(entity_dict.values())
        flattened_json = self.downloader.convert_json(entity_list)
        workbook = self.downloader.create_workbook(flattened_json)
        return workbook

    def link_spreadsheet(self, submission_url, submission, filename):
        schema_url = self.ingest_api.get_latest_schema_url('type', 'file', 'supplementary_file')
        spreadsheet_payload = self.build_supplementary_file_payload(schema_url, filename)
        submission_files_url = self.ingest_api.get_link_in_submission(submission_url, 'files')
        file_entity_response = self.ingest_api.post(submission_files_url, json=spreadsheet_payload)
        file_entity = file_entity_response.json()
        projects = self.ingest_api.get_related_entities(
            entity=submission,
            relation='projects',
            entity_type='projects'
        )
        project_entity = next(projects)
        self.ingest_api.link_entity(
            from_entity=project_entity,
            to_entity=file_entity,
            relationship='supplementaryFiles'
        )

    def update_spreadsheet_finish(self, create_date:datetime, submission_url:str, job_id:str):
        finished_date = datetime.now(timezone.utc)
        self.__patch_file_generation(submission_url, create_date, job_id, finished_date)

    def copy_to_s3_staging_area(self, spreadsheet_details: SpreadsheetDetails, staging_area):
        staging_area_url = urlparse(staging_area)
        bucket = staging_area_url.netloc
        object_name = f'{staging_area_url.path.lstrip("/")}/{spreadsheet_details.filename}'
        s3_client = self.init_s3_client()
        response = None
        try:
            response = s3_client.upload_file(Filename=spreadsheet_details.filepath,
                                             Bucket=bucket,
                                             Key=object_name)
            self.logger.info(f'uploaded metadata spreadsheet {spreadsheet_details.filename} '
                             f'to upload area {staging_area}')
        except ClientError as e:
            self.logger.error(f's3 response: {response}', e)

    def init_s3_client(self):
        return boto3.client(
            's3',
            aws_access_key_id=self.config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=self.config['AWS_ACCESS_KEY_SECRET']
        )

    def __patch_file_generation(self, submission_url, create_date: datetime, job_id: str, finished_date=None):
        patch = {
            'lastSpreadsheetGenerationJob': {
                'finishedDate': date_to_json_string(finished_date) if finished_date else None,
                'createdDate': date_to_json_string(create_date),
                'jobId': job_id
            }
        }
        self.ingest_api.patch(submission_url, json=patch)

    @staticmethod
    def get_spreadsheet_details(create_date, storage_dir, submission_uuid) -> SpreadsheetDetails:
        directory = f'{storage_dir}/{submission_uuid}/downloads'
        timestamp = create_date.strftime("%Y%m%d-%H%M%S")
        filename = f'{submission_uuid}_{timestamp}.xlsx'
        filepath = f'{directory}/{filename}'

        return SpreadsheetDetails(filename, filepath, directory)

    @staticmethod
    def save_spreadsheet(spreadsheet_details: SpreadsheetDetails, workbook):
        os.makedirs(spreadsheet_details.directory, exist_ok=True)
        workbook.save(spreadsheet_details.filepath)

    @staticmethod
    def build_supplementary_file_payload(schema_url, filename):
        return {
            "describedBy": schema_url,
            "schema_type": "file",
            "file_core": {
                "file_name": filename,
                "format": "xlsx",
                "file_source": "DCP/2 Ingest",
                "content_description": {
                    "text": "metadata spreadsheet",
                    "ontology": "data:2193",
                    "ontology_label": "Database entry metadata"
                }
            }
        }
