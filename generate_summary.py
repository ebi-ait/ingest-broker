import argparse
import uuid

import jsonpickle
from hca_ingest.api.ingestapi import IngestApi

from broker.common.util.tsv_summary_util import TSVSummaryUtil
from broker.service.summary_service import SummaryService


def generate_submission_summary(uuid, ingest_url):
    ingest_api = IngestApi(ingest_url)
    submission = ingest_api.getSubmissionByUuid(uuid)
    summary = SummaryService(ingest_api).summary_for_submission(submission)

    return summary


def generate_project_summary(uuid, ingest_url):
    ingest_api = IngestApi(ingest_url)
    project = ingest_api.getProjectByUuid(uuid)
    summary = SummaryService(ingest_api).summary_for_project(project)

    return summary


def check_uuid(uuid_str):
    try:
        uuid.UUID(uuid_str, version=4)
        return uuid_str
    except:
        raise argparse.ArgumentTypeError("%s is an invalid uuid value" % uuid_str)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('ingest_api', metavar='H', nargs=1,
                        help='the url of the ingest API (e.g https://api.ingest.dev.archive.data.humancellatlas.org)')
    parser.add_argument('summary_type', metavar='T', nargs=1, choices=['project', 'submission'],
                        help='the type of summary (project or submission)')
    parser.add_argument('uuid', metavar='U', nargs=1, type=check_uuid,
                        help='the uuid of the project/submission')
    parser.add_argument('output_format', metavar='O', nargs=1, choices=['json', 'tsv'],
                        help='summary output format')

    args = parser.parse_args()

    uuid_str = args.uuid[0]
    summary_type = args.summary_type[0]
    ingest_url = args.ingest_api[0]
    output_format = args.output_format[0]

    summary = generate_project_summary(uuid_str, ingest_url) if summary_type == 'project' else generate_submission_summary(uuid_str, ingest_url)

    if output_format == 'json':
        print(jsonpickle.encode(summary, unpicklable=False))
    elif output_format == 'tsv':
        if summary_type == 'project':
            TSVSummaryUtil.project_summary_to_tsv(summary)
        else:
            TSVSummaryUtil.submission_summary_to_tsv(summary)



