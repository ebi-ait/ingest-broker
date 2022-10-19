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
        self.config = {
            "AWS_ACCESS_KEY_ID": config['AWS_ACCESS_KEY_ID'],
            "AWS_ACCESS_KEY_SECRET": config['AWS_ACCESS_KEY_SECRET'],
            "AWS_ROLE": config['AWS_ROLE']
        }

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
            staging_area = submission['stagingDetails']['stagingAreaLocation']['value']
            create_date = self.update_spreadsheet_start(submission_url, job_id)
            spreadsheet_details = self.get_spreadsheet_details(create_date, storage_dir, submission_uuid)
            workbook = self.export(submission_uuid)
            self.save_spreadsheet(spreadsheet_details, workbook)
            self.link_spreadsheet(submission_url, submission, spreadsheet_details.filename)
            self.update_spreadsheet_finish(create_date, submission_url, job_id)
            self.copy_to_s3_staging_area(spreadsheet_details, staging_area)
            self.logger.info(f'Done exporting spreadsheet for submission {submission_uuid}!')
        except Exception as e:
            err = f'Problem when generating spreadsheet for submission with uuid {submission_uuid}: {str(e)}'
            self.logger.error(err, e)
            raise Exception(err) from e

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
        file_entity = self.ingest_api.create_file(submission_url, filename=filename, content=spreadsheet_payload)
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

    def update_spreadsheet_finish(self, create_date: datetime, submission_url: str, job_id: str):
        finished_date = datetime.now(timezone.utc)
        self.__patch_file_generation(submission_url, create_date, job_id, finished_date)

    def copy_to_s3_staging_area(self, spreadsheet_details: SpreadsheetDetails, staging_area):
        staging_area_url = urlparse(staging_area)
        bucket = staging_area_url.netloc
        object_name = f'{staging_area_url.path.strip("/")}/{spreadsheet_details.filename}'
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

    def init_s3_client(self) -> boto3.client:
        # The calls to AWS STS AssumeRole must be signed with the access key ID
        # and secret access key of an existing IAM user or by using existing temporary
        # credentials such as those from another role. (You cannot call AssumeRole
        # with the access key for the root account.) The credentials can be in
        # environment variables or in a configuration file and will be discovered
        # automatically by the boto3.client() function. For more information, see the
        # Python SDK documentation:
        # http://boto3.readthedocs.io/en/latest/reference/services/sts.html#client

        # create an STS client object that represents a live connection to the
        # STS service
        sts_client = boto3.client(
            'sts',
            aws_access_key_id=self.config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=self.config['AWS_ACCESS_KEY_SECRET']
        )

        # Call the assume_role method of the STSConnection object and pass the role ARN
        assumed_role_object = sts_client.assume_role(
            RoleArn=self.config["AWS_ROLE"],
            RoleSessionName="dcp-upload-submitter"
        )

        # From the response that contains the assumed role, get the temporary
        # credentials that can be used to make subsequent API calls
        credentials = assumed_role_object['Credentials']

        # Use the temporary credentials that AssumeRole returns to make a
        # connection to Amazon S3
        return boto3.client(
            's3',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
        )

    def __patch_file_generation(self, submission_url, create_date: datetime, job_id: str, finished_date=None):
        patch = self.build_generation_job(create_date, job_id, finished_date)
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
    def build_generation_job(create_date, job_id, finished_date):
        return {
            'lastSpreadsheetGenerationJob': {
                'finishedDate': date_to_json_string(finished_date) if finished_date else None,
                'createdDate': date_to_json_string(create_date),
                'jobId': job_id
            }
        }

    @staticmethod
    def build_supplementary_file_payload(schema_url, filename):
        return {
            "describedBy": schema_url,
            "schema_type": "file",
            "file_core": {
                "file_name": filename,
                "format": "xlsx",
                "file_source": "DCP/2 Ingest",
                "content_description": [
                    {
                        "text": "metadata spreadsheet",
                        "ontology": "data:2193",
                        "ontology_label": "Database entry metadata"
                    }
                ]
            }
        }
