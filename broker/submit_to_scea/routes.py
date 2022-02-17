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
from broker.submit_to_scea.exceptions import SceaHttpError, SceaConvertorRunError, GenerateSceaFilesError

import_geo_bp = Blueprint(
    'submit_to_scea', __name__, url_prefix='/'
)

LOGGER = logging.getLogger(__name__)


@submit_to_scea_bp.route('/submit-to-scea', methods=['POST'])
@cross_origin(expose_headers=['Content-Disposition'])
def get_files_from_scea():

    try:

        scea_accession = request.args.get('accession_number')
        spreadsheet = request.args.get('spreadsheet')
        project_uuid = request.args.get('project_uuid')
        study = request.args.get('study')
        name =  request.args.get('name') # optional argument
        curators = request.args.get('curators')
        technology_type = request.args.get('technology_type')
        experiment_type = request.args.get('experiment_type')
        facs = request.args.get('facs') # optional argument
        experimental_factors = request.args.get('experimental_factors')
        public_release_date = request.args.get('public_release_date')
        hca_update_date = request.args.get('hca_update_date')
        related_scea_accession = request.args.get('related_scea_accession') # optional argument
        output_dir = request.args.get('output_dir')

        output_dir = _generate_scea_files([scea_accession,
                                                spreadsheet,
                                                project_uuid,
                                                study,
                                                name,
                                                curators,
                                                technology_type,
                                                experiment_type,
                                                facs,
                                                experimental_factors,
                                                public_release_date,
                                                hca_update_date,
                                                related_scea_accession,
                                                output_dir])

        idf_name_name = 'E-HCAD-' + str(scea_accession) + '.idf.txt'
        _send_from_directory(idf_file_name, output_dir)

        sdrf_name_name = 'E-HCAD-' + str(scea_accession) + '.sdrf.txt'
        _send_from_directory(sdrf_file_name, output_dir)

    except Exception as e:
    LOGGER.exception(e)
    raise SceaHttpError(f'Unable to convert the HCA metadata sheet to SCEA format.')


@submit_to_scea_bp.errorhandler(SceaHttpError)
def handle_submit_to_scea_http_error(e: SceaHttpError):
    return response_json(e.status_code, {'message': e.message})


def _send_from_directory_idf(idf_file_name, output_dir):

    tmpdirname = tempfile.TemporaryDirectory()

    return send_from_directory(tmpdirname,
                        idf_file,
                        as_attachment=True,
                        cache_timeout=0)


def _send_from_directory_sdrf(sdrf_file_name, output_dir):

    tmpdirname = tempfile.TemporaryDirectory()

    return send_from_directory(tmpdirname,
                        sdrf_file,
                        as_attachment=True,
                        cache_timeout=0)
