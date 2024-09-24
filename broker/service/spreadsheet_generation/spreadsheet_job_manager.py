from dataclasses import dataclass
from typing import Dict, BinaryIO, Optional
import json
from enum import Enum
from concurrent.futures import ThreadPoolExecutor

from broker.service.spreadsheet_generation.spreadsheet_generator import SpreadsheetGenerator, SpreadsheetSpec
import logging


class JobStatus(Enum):
    STARTED = "STARTED"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


@dataclass
class JobSpec:
    status: JobStatus
    job_id: str
    spreadsheet_path: str
    filename: str

    @staticmethod
    def from_dict(data: Dict) -> 'JobSpec':
        try:
            return JobSpec(JobStatus[data["status"]], data["job_id"], data["spreadsheet_path"], data["filename"])
        except:
            raise

    def to_dict(self) -> Dict:
        return {
            "status": self.status.value,
            "job_id": self.job_id,
            "spreadsheet_path": self.spreadsheet_path,
            "filename": self.filename
        }


class SpreadsheetJobManager:
    def __init__(self, spreadsheet_generator: SpreadsheetGenerator, output_dir_path: str, worker_pool: Optional[ThreadPoolExecutor]=None):
        self.spreadsheet_generator = spreadsheet_generator
        self.output_dir_path = output_dir_path
        self.worker_pool = worker_pool if worker_pool is not None else ThreadPoolExecutor(5)

        self.logger = logging.getLogger(__name__)

    def create_job(self, spreadsheet_spec: SpreadsheetSpec, filename: str, experiment_type: str = None) -> JobSpec:
        job_id = spreadsheet_spec.hashcode()
        spreadsheet_output_path = f'{self.output_dir_path}/{job_id}.xlsx'
        job_spec = JobSpec(JobStatus.STARTED, job_id, spreadsheet_output_path, filename)
        job_spec_path = f'{self.output_dir_path}/{job_id}.json'

        with open(job_spec_path, "w") as job_spec_file:
            json.dump(job_spec.to_dict(), job_spec_file)

        self.worker_pool.submit(lambda: self._do_create_spreadsheet_job(spreadsheet_spec, job_spec_path, spreadsheet_output_path, experiment_type))

        return job_spec

    def _do_create_spreadsheet_job(self, spreadsheet_spec: SpreadsheetSpec, job_spec_path: str, output_path: str, experiment_type: str = None):
        job_result = self._maybe_create_spreadsheet(spreadsheet_spec, output_path, experiment_type)
        job_spec = self.load_job_spec_from_path(job_spec_path)
        completed_job_spec = JobSpec(job_result, job_spec.job_id, output_path, job_spec.filename)
        self.write_job_spec(completed_job_spec, job_spec_path)

    def _maybe_create_spreadsheet(self, spreadsheet_spec: SpreadsheetSpec, output_path: str, experiment_type: str = None) -> JobStatus:
        try:
            self.spreadsheet_generator.generate(spreadsheet_spec, output_path, experiment_type)
            return JobStatus.COMPLETE
        except Exception as e:
            self.logger.exception(e)
            return JobStatus.ERROR

    def status_for_job(self, job_id: str) -> JobStatus:
        return self.load_job_spec(job_id).status

    def spreadsheet_for_job(self, job_id) -> BinaryIO:
        spreadsheet_path = self.load_job_spec(job_id).spreadsheet_path
        return open(spreadsheet_path, "rb")

    def load_job_spec(self, job_id) -> JobSpec:
        return SpreadsheetJobManager.load_job_spec_from_path(f'{self.output_dir_path}/{job_id}.json')

    @staticmethod
    def load_job_spec_from_path(job_spec_path: str) -> JobSpec:
        with open(job_spec_path, "r") as job_spec_file:
            return JobSpec.from_dict(json.load(job_spec_file))

    @staticmethod
    def write_job_spec(job_spec: JobSpec, job_spec_path: str):
        with open(job_spec_path, "w") as job_spec_file:
            json.dump(job_spec.to_dict(), job_spec_file)

