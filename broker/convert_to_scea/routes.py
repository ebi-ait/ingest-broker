import logging
import re
import tempfile
from http import HTTPStatus

from flask import Blueprint, send_file, request
from flask_cors import cross_origin
from hca_to_scea import hca2scea
from ingest.api.ingestapi import IngestApi

from ingest.importer.importer import XlsImporter
from broker.common.util import response_json
from broker.convert_to_scea.exceptions import ImportSCEAHttpError, GenerateSCEAFilesError

convert_to_scea_bp = Blueprint(
    'convert_to_scea', __name__, url_prefix='/'
)

LOGGER = logging.getLogger(__name__)


@convert_to_scea_bp.route('/convert_to_scea', methods=['POST'])
@cross_origin(expose_headers=['Content-Disposition'])
def get_scea_files():

    project_uuid = request.args.get('project_uuid')
    accession_num = request.args.get('accession_num')
    curator = request.args.get('curator')
    experiment_type = request.args.get('experiment_type')
    factor values = request.args.get('factor values')
    public_release_date = request.args.get('public_release_date')
    hca_release_date = request.args.get('hca_release_date')
    study_accession = request.args.get('study_accession')

    output_file = _generate_scea_files(
    project_uuid,
    accession_num,
    curator,
    experiment_type,
    factor_values,
    public_release_date,
    hca_release_date,
    study_accession)
    filename = f'E-HCAD-{accession_num}.zip'

    return _send_file(filename, output_file)

@convert_to_scea_bp.errorhandler(ImportSCEAHttpError)
def handle_convert_to_scea_http_error(e: ImportSCEAHttpError):
    return response_json(e.status_code, {'message': e.message})

def _generate_scea_files(project_uuid,
                        accession_num,
                        curator,
                        experiment_type,
                        factor_values,
                        public_release_date,
                        hca_release_date,
                        study_accession):

    try:
        output_file = hca_to_scea.hca2scea("spreadsheet"=spreadsheet,"project_uuid"=project_uuid,
        "accession_number"=accession_num,"curators"=curator,"experiment_type"=experiment_type,
        experimental_factors="factor_values","public_release_date"=public_release_date,
        "hca_update_date"=hca_release_date,"study"=study_accession)
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
