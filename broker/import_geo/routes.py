# --- core imports
import logging
import re
import tempfile
from http import HTTPStatus

# --- third-party imports
from flask import Blueprint, send_file, request
from flask import current_app as app
from flask_cors import cross_origin
from geo_to_hca import geo_to_hca
# --- application imports
from ingest.importer.importer import XlsImporter

from broker.common.util import response_json

import_geo_bp = Blueprint(
    'import_geo', __name__, url_prefix='/'
)

LOGGER = logging.getLogger(__name__)


def is_valid_geo_accession(geo_accession):
    regex = re.compile("^GSE.*$")
    return bool(regex.match(geo_accession))


@import_geo_bp.route("/import-geo", methods=['POST'])
@cross_origin()
def get_spreadsheet_using_geo():
    if request.method == 'OPTIONS':
        return response_json(HTTPStatus.OK, {})
    else:
        args = request.args
        geo_accession = args.get('accession')

        if not is_valid_geo_accession(geo_accession):
            return response_json(HTTPStatus.BAD_REQUEST, "The given geo accession is invalid")

        try:
            workbook = geo_to_hca.create_spreadsheet_using_geo_accession(geo_accession)

            if workbook:
                importer = XlsImporter(app.ingest_api)
                token = request.headers.get('Authorization')
                project_uuid, errors = importer.import_project_from_workbook(workbook, token)
                if project_uuid:
                    LOGGER.info(f'A project with uuid {project_uuid} was created.')
                else:
                    error_details = [e.get('details') for e in errors]
                    # TODO How do we let users know?
                    LOGGER.error(f'There were errors in importing the project metadata: {error_details}')
                temp_file = tempfile.NamedTemporaryFile()
                filename = f"hca_metadata_spreadsheet-{geo_accession}.xlsx"
                workbook.properties.write_properties({'description': project_uuid})
                workbook.save(temp_file.name)

                # TODO send (project uuid or errors) and file
                return send_file(temp_file.name,
                                 as_attachment=True,
                                 cache_timeout=0,
                                 attachment_filename=filename)

        except Exception as e:
            LOGGER.exception(e)
            return response_json(HTTPStatus.INTERNAL_SERVER_ERROR, "Unable to find HCA metadata against given accession")
