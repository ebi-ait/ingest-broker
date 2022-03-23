import logging
import re
import tempfile
from http import HTTPStatus

from flask import Blueprint, send_file, request
from flask_cors import cross_origin
from hca2scea import hca2scea
from ingest.api.ingestapi import IngestApi

from ingest.importer.importer import XlsImporter
from broker.common.util import response_json
from broker.convert_to_scea.exceptions import ImportSCEAHttpError, GenerateSCEAFilesError

get_scea_files_bp = Blueprint(
    'get-scea-files', __name__, url_prefix='/'
)

LOGGER = logging.getLogger(__name__)


@convert_to_scea_bp.route('/get-scea-files', methods=['POST'])
@cross_origin(expose_headers=['Content-Disposition'])
def get_scea_files():

    params_object = request.args.get('params_object')

    output_file = _generate_scea_files(params_object)
    filename = f'E-HCAD-{accession_num}.zip'

    return _send_file(filename, output_file)

@convert_to_scea_bp.errorhandler(ImportSCEAHttpError)
def handle_convert_to_scea_http_error(e: ImportSCEAHttpError):
    return response_json(e.status_code, {'message': e.message})

def _generate_scea_files(params_object):

    try:
        output_file = hca2scea.hca2scea(
        "spreadsheet"=params_object['spreadsheet'],
        "project_uuid"=params_object['project_uuid'],
        "accession_num"=params_object['accession_num'],
        "curator"=params_object['curator'],
        "experiment_type"=params_object['experiment_type'],
        "factor_values"=params_object['factor_values'],
        "public_release_date"=params_object['public_release_date'],
        "hca_update_date"=params_object['hca_update_date'],
        "output_dir"=params_object['output_dir'],
        "zip_format"=params_object['zip_format']
        )

    except Exception as e:
        LOGGER.exception(e)
        raise GenerateSCEAFilesError(f'Unable to convert the dataset to SCEA files. [{repr(e)}]')

    return output_file

def _send_file(filename, output_file):
    temp_file = tempfile.NamedTemporaryFile()
    output_file.save(temp_file.name)
    return send_file(temp_file.name,
                     as_attachment=True,
                     cache_timeout=0,
                     attachment_filename=filename)
