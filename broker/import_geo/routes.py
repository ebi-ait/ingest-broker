import logging
import re
import tempfile
from http import HTTPStatus

from flask import Blueprint, send_file, request
from flask import current_app as app
from flask_cors import cross_origin
from geo_to_hca import geo_to_hca

from ingest.importer.importer import XlsImporter
from broker.common.util import response_json
from broker.import_geo.exceptions import ImportGeoHttpError, InvalidGeoAccession, GenerateGeoWorkbookError, \
    ImportProjectWorkbookError

import_geo_bp = Blueprint(
    'import_geo', __name__, url_prefix='/'
)

LOGGER = logging.getLogger(__name__)


@import_geo_bp.route('/import-geo', methods=['POST'])
@cross_origin(expose_headers=['Content-Disposition'])
def get_spreadsheet_using_geo():
    geo_accession = request.args.get('accession')

    workbook = _generate_geo_workbook(geo_accession)
    filename = f'hca_metadata_spreadsheet-{geo_accession}.xlsx'

    return _send_file(filename, workbook)


@import_geo_bp.route('/import-geo-project', methods=['POST'])
@cross_origin(expose_headers=['Content-Disposition'])
def import_project_using_geo():
    geo_accession = request.args.get('accession')

    workbook = _generate_geo_workbook(geo_accession)

    project_uuid = _import_project_from_workbook(workbook)

    return response_json(HTTPStatus.OK, {'project_uuid': project_uuid})


@import_geo_bp.errorhandler(ImportGeoHttpError)
def handle_import_geo_http_error(e: ImportGeoHttpError):
    return response_json(e.status_code, {'message': e.message})


def _generate_geo_workbook(geo_accession: str):
    if not _is_valid_geo_accession(geo_accession):
        raise InvalidGeoAccession('The given geo accession is invalid.')

    try:
        workbook = geo_to_hca.create_spreadsheet_using_geo_accession(geo_accession)
    except Exception as e:
        raise GenerateGeoWorkbookError('Unable to find HCA metadata against given accession.')

    return workbook


def _import_project_from_workbook(workbook):
    importer = XlsImporter(app.ingest_api)
    token = request.headers.get('Authorization')
    project_uuid, errors = importer.import_project_from_workbook(workbook, token)

    if errors:
        error_details = [e.get('details') for e in errors if e.get('details')]
        error_messages = ' ,'.join(error_details)
        raise ImportProjectWorkbookError(f'There were errors in importing the project: {error_messages}.')

    return project_uuid


def _is_valid_geo_accession(geo_accession):
    regex = re.compile('^GSE.*$')
    return bool(regex.match(geo_accession))


def _send_file(filename, workbook):
    temp_file = tempfile.NamedTemporaryFile()
    workbook.save(temp_file.name)
    return send_file(temp_file.name,
                     as_attachment=True,
                     cache_timeout=0,
                     attachment_filename=filename)
